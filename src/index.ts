import type { Redis } from "ioredis";
import { stringify, parse } from "superjson";
import type { ZodSchema } from "zod";
import { createDeferredPromise, PubSubDeferredPromise, pubsubDeferredPromise } from "./promise";

export interface RedisPubSubOptions {
  publisher: Redis;
  subscriber: Redis;
  onParseError?: (err: unknown) => void;
}

export function RedisPubSub({
  publisher,
  subscriber,
  onParseError = console.error,
}: RedisPubSubOptions) {
  type DataPromise = {
    current: PubSubDeferredPromise<unknown>;
    unsubscribe: () => Promise<void>;
  };
  type SubscriptionValue = Readonly<{
    name: string;
    channel: string;
    isPattern: boolean;
    schema: ZodSchema<unknown>;
    dataPromises: Set<DataPromise>;
    ready: Promise<void>;
  }>;
  const subscriptionsMap: Record<string, SubscriptionValue> = {};

  const onPMessageHandler = onMessage.bind(void 0);
  const onMessageHandler = onMessage.bind(void 0, undefined);

  subscriber.on("pmessage", onPMessageHandler);
  subscriber.on("message", onMessageHandler);

  const subscribedChannels: Record<string, boolean | Promise<void>> = {};
  const unsubscribingChannels: Record<string, Promise<void> | false> = {};

  return {
    createChannel,
    unsubscribeAll,
    close,
  };

  async function onMessage(pattern: string | undefined, channel: string, message: string) {
    const subscription = subscriptionsMap[pattern || channel];

    if (!subscription?.dataPromises.size) return;

    let parsedMessage: unknown;

    try {
      parsedMessage = await subscription.schema.parseAsync(parse(message));
    } catch (err) {
      onParseError(err);
      return;
    }

    for (const dataPromise of subscription.dataPromises) {
      dataPromise.current.values.push(parsedMessage);
      dataPromise.current.resolve();
    }
  }

  function redisSubscribe({ channel, isPattern }: { channel: string; isPattern: boolean }) {
    const subscribed = subscribedChannels[channel];

    if (subscribed) {
      if (typeof subscribed === "boolean") return;

      return subscribed;
    }
    return (subscribedChannels[channel] = subscriber[isPattern ? "psubscribe" : "subscribe"](
      channel
    ).then(
      () => {
        subscribedChannels[channel] = true;
      },
      (err) => {
        subscribedChannels[channel] = false;

        throw err;
      }
    ));
  }

  function redisUnsubscribe({ channel, isPattern }: { channel: string; isPattern: boolean }) {
    const unsubscribing = unsubscribingChannels[channel];

    if (unsubscribing) return unsubscribing;

    const subcribed = subscribedChannels[channel];

    if (!subcribed) return;

    if (typeof subcribed === "boolean") {
      return unsubscribe();
    }

    return subcribed.then(unsubscribe);

    function unsubscribe() {
      return (unsubscribingChannels[channel] = subscriber[
        isPattern ? "punsubscribe" : "unsubscribe"
      ](channel).then(
        () => {
          unsubscribingChannels[channel] = subscribedChannels[channel] = false;
        },
        (err) => {
          unsubscribingChannels[channel] = false;
          throw err;
        }
      ));
    }
  }

  async function unsubscribeAll() {
    await Promise.all(
      Object.values(subscriptionsMap).map(({ dataPromises, name: channel, isPattern }) =>
        Promise.all(Array.from(dataPromises).map((v) => v.unsubscribe())).then(() =>
          redisUnsubscribe({
            channel,
            isPattern,
          })
        )
      )
    );
  }

  function createChannel<Value>({
    schema,
    name,
    isLazy = true,
    subscriptionPattern,
  }: {
    schema: ZodSchema<Value>;
    /**
     * Channel name
     *
     * For publish:
     * - If `specifier` argument **is not** specified on `.publish`, it's used as channel trigger
     * - If `specifier` argument **is** specified on `.publish`, it's used as a prefix of the `specifier` in the channel trigger
     *
     * For subscription:
     * - If `subscriptionPattern` **is not** specified, it's used as channel trigger on `.subscribe`
     * - If `subscriptionPattern` **is** specified, it's overriden by it
     */
    name: string;
    /**
     * Subscription trigger pattern to be used on subscription, overrides `name` on `.subscribe`
     */
    subscriptionPattern?: string;
    /**
     * @default true
     */
    isLazy?: boolean;
  }) {
    const channel = subscriptionPattern ?? name;
    const isPattern = subscriptionPattern != null;

    const dataPromises = new Set<DataPromise>();

    let readyPromise = createDeferredPromise();

    if (!isLazy) {
      redisSubscribe({
        channel,
        isPattern,
      })?.then(readyPromise.resolve, readyPromise.reject);
    }

    const subscriptionValue = (subscriptionsMap[channel] = {
      dataPromises,
      name,
      channel,
      schema,
      ready: readyPromise.promise,
      isPattern,
    });

    function subscribe<FilteredValue extends Value>(subscribeArguments: {
      abortSignal?: AbortSignal;
      filter: (value: Value) => value is FilteredValue;
    }): AsyncGenerator<FilteredValue, void, unknown>;
    function subscribe(subscribeArguments?: {
      abortSignal?: AbortSignal;
      filter?: (value: Value) => unknown;
    }): AsyncGenerator<Value, void, unknown>;
    async function* subscribe({
      abortSignal,
      filter,
    }: {
      abortSignal?: AbortSignal;
      filter?: (value: Value) => unknown;
    } = {}) {
      let abortListener: (() => void) | undefined;

      if (abortSignal) {
        abortSignal.addEventListener(
          "abort",
          (abortListener = () => {
            unsubscribe().catch((err) => readyPromise.reject(err));
          })
        );
      }

      const dataPromise: DataPromise = {
        current: pubsubDeferredPromise(),
        unsubscribe,
      };

      async function unsubscribe() {
        dataPromise.current.isDone = true;
        dataPromise.current.resolve();
        dataPromises.delete(dataPromise);

        if (abortSignal && abortListener) {
          abortSignal.removeEventListener("abort", abortListener);
        }

        if (isLazy && dataPromises.size === 0) {
          readyPromise = createDeferredPromise();
          subscriptionValue.ready = readyPromise.promise;

          await redisUnsubscribe({
            channel,
            isPattern: false,
          });
          return;
        }
      }

      dataPromises.add(dataPromise);

      const subscribing = redisSubscribe({
        channel,
        isPattern,
      })?.then(readyPromise.resolve, readyPromise.reject);

      if (subscribing) await subscribing;

      while (true) {
        await dataPromise.current.promise;

        for (const value of dataPromise.current.values as Value[]) {
          if (filter && !filter(value)) continue;

          yield value;
        }

        if (dataPromise.current.isDone) {
          break;
        } else {
          dataPromise.current = pubsubDeferredPromise();
        }
      }

      await dataPromise.unsubscribe();
    }

    return {
      get ready() {
        return readyPromise.promise;
      },
      subscribe,
      async publish(
        ...values: [{ value: Value; specifier?: string }, ...{ value: Value; specifier?: string }[]]
      ) {
        await Promise.all(
          values.map(async ({ value, specifier }) => {
            let parsedValue: Value;

            try {
              parsedValue = await schema.parseAsync(value);
            } catch (err) {
              onParseError(err);
              return;
            }

            await publisher.publish(specifier ? name + specifier : name, stringify(parsedValue));
          })
        );
      },
      async unsubscribeAll() {
        await Promise.all(Array.from(dataPromises.values()).map((v) => v.unsubscribe()));
        await redisUnsubscribe({
          channel,
          isPattern,
        });
      },
    };
  }

  async function close() {
    subscriber.off("pmessage", onPMessageHandler);
    subscriber.off("message", onMessageHandler);

    await unsubscribeAll();

    subscriber.disconnect();
    publisher.disconnect();
  }
}
