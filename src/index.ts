import type { Redis } from "ioredis";
import { parse, stringify } from "superjson";
import type { ZodSchema } from "zod";
import {
  createDeferredPromise,
  DeferredPromise,
  PubSubDeferredPromise,
  pubsubDeferredPromise,
} from "./promise";

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
  type SubscriptionValue = {
    readonly name: string;
    readonly specifier: string | undefined;
    readonly channel: string;
    readonly schema: ZodSchema<unknown>;
    readonly dataPromises: Set<DataPromise>;
    ready: DeferredPromise<void>;
  };
  const subscriptionsMap: Record<string, SubscriptionValue> = {};

  subscriber.on("message", onMessage);

  const subscribedChannels: Record<string, boolean | Promise<void>> = {};
  const unsubscribingChannels: Record<string, Promise<void> | false> = {};

  return {
    createChannel,
    unsubscribeAll,
    close,
  };

  async function onMessage(channel: string, message: string) {
    const subscription = subscriptionsMap[channel];

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

  function redisSubscribe({ channel }: { channel: string }) {
    const subscribed = subscribedChannels[channel];

    if (subscribed) {
      if (typeof subscribed === "boolean") return;

      return subscribed;
    }
    return (subscribedChannels[channel] = subscriber.subscribe(channel).then(
      () => {
        subscribedChannels[channel] = true;
      },
      (err) => {
        subscribedChannels[channel] = false;

        throw err;
      }
    ));
  }

  function redisUnsubscribe({ channel }: { channel: string }) {
    const unsubscribing = unsubscribingChannels[channel];

    if (unsubscribing) return unsubscribing;

    const subcribed = subscribedChannels[channel];

    if (!subcribed) return;

    if (typeof subcribed === "boolean") {
      return unsubscribe();
    }

    return subcribed.then(unsubscribe);

    function unsubscribe() {
      return (unsubscribingChannels[channel] = subscriber.unsubscribe(channel).then(
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
    const subscriptions = Object.values(subscriptionsMap);

    await Promise.all(
      subscriptions.flatMap(({ dataPromises, channel }) => [
        ...Array.from(dataPromises).map(({ unsubscribe }) => unsubscribe()),
        redisUnsubscribe({
          channel,
        }),
      ])
    );
  }

  function createChannel<Value>({
    schema,
    name,
    isLazy = true,
  }: {
    schema: ZodSchema<Value>;
    name: string;
    /**
     * @default true
     */
    isLazy?: boolean;
  }) {
    if (!isLazy) {
      const channel = name;
      const initialSubscriptionValue = (subscriptionsMap[name] = {
        dataPromises: new Set<DataPromise>(),
        name,
        channel,
        specifier: undefined,
        schema,
        ready: createDeferredPromise(),
      });
      redisSubscribe({
        channel,
      })?.then(initialSubscriptionValue.ready.resolve, initialSubscriptionValue.ready.reject);
    }

    function subscribe<FilteredValue extends Value>(subscribeArguments: {
      abortSignal?: AbortSignal;
      filter: (value: Value) => value is FilteredValue;
      specifier?: string | number;
    }): AsyncGenerator<FilteredValue, void, unknown>;
    function subscribe(subscribeArguments?: {
      abortSignal?: AbortSignal;
      filter?: (value: Value) => unknown;
      specifier?: string | number;
    }): AsyncGenerator<Value, void, unknown>;
    async function* subscribe({
      abortSignal,
      filter,
      specifier,
    }: {
      abortSignal?: AbortSignal;
      filter?: (value: Value) => unknown;
      specifier?: string | number;
    } = {}) {
      const channel = specifier ? name + specifier : name;

      const subscriptionValue = (subscriptionsMap[channel] ||= {
        schema,
        name,
        specifier: specifier?.toString(),
        channel,
        dataPromises: new Set(),
        ready: createDeferredPromise(),
      });

      const dataPromises = subscriptionValue.dataPromises;

      let abortListener: (() => void) | undefined;

      if (abortSignal) {
        abortSignal.addEventListener(
          "abort",
          (abortListener = () => {
            unsubscribe().catch((err) => subscriptionValue.ready.reject(err));
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
          subscriptionValue.ready = createDeferredPromise();

          await redisUnsubscribe({
            channel,
          });
          return;
        }
      }

      dataPromises.add(dataPromise);

      const subscribing = redisSubscribe({
        channel,
      })?.then(subscriptionValue.ready.resolve, subscriptionValue.ready.reject);

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
        const channel = name;
        return (subscriptionsMap[channel] ||= {
          dataPromises: new Set<DataPromise>(),
          name,
          specifier: undefined,
          channel,
          schema,
          ready: createDeferredPromise(),
        }).ready.promise;
      },
      subscribe,
      async publish(
        ...values: [
          { value: Value; specifier?: string | number },
          ...{ value: Value; specifier?: string | number }[]
        ]
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
        const subscriptions = Object.values(subscriptionsMap).filter(
          (value) => value.name === name
        );

        await Promise.all(
          subscriptions.flatMap(({ dataPromises, channel }) => [
            ...Array.from(dataPromises).map(({ unsubscribe }) => unsubscribe()),
            redisUnsubscribe({
              channel,
            }),
          ])
        );
      },
    };
  }

  async function close() {
    subscriber.off("message", onMessage);

    await unsubscribeAll();

    subscriber.disconnect();
    publisher.disconnect();
  }
}
