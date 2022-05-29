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
    channel: string;
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

  function redisSubscribe({ channel, pattern }: { channel: string; pattern: boolean }) {
    const subscribed = subscribedChannels[channel];

    if (subscribed) {
      if (typeof subscribed === "boolean") return;

      return subscribed;
    }
    return (subscribedChannels[channel] = subscriber[pattern ? "psubscribe" : "subscribe"](
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

  function redisUnsubscribe({ channel, pattern }: { channel: string; pattern: boolean }) {
    const unsubscribing = unsubscribingChannels[channel];

    if (unsubscribing) return unsubscribing;

    const subcribed = subscribedChannels[channel];

    if (!subcribed) return;

    if (typeof subcribed === "boolean") {
      return unsubscribe();
    }

    return subcribed.then(unsubscribe);

    function unsubscribe() {
      return (unsubscribingChannels[channel] = subscriber[pattern ? "punsubscribe" : "unsubscribe"](
        channel
      ).then(
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
      Object.values(subscriptionsMap).map(({ dataPromises, channel }) =>
        Promise.all(Array.from(dataPromises).map((v) => v.unsubscribe())).then(() =>
          redisUnsubscribe({
            channel,
            pattern: false,
          })
        )
      )
    );
  }

  function createChannel<Output>({
    schema,
    name,
    lazy = true,
  }: {
    schema: ZodSchema<Output>;
    name: string;
    /**
     * @default true
     */
    lazy?: boolean;
  }) {
    const dataPromises = new Set<DataPromise>();

    let readyPromise = createDeferredPromise();

    if (!lazy) {
      redisSubscribe({
        channel: name,
        pattern: false,
      })?.then(readyPromise.resolve, readyPromise.reject);
    }

    const subscriptionValue = (subscriptionsMap[name] = {
      dataPromises,
      channel: name,
      schema,
      ready: readyPromise.promise,
    });

    return {
      get ready() {
        return readyPromise.promise;
      },
      async *subscribe({
        abortSignal,
      }: {
        abortSignal?: AbortSignal;
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

          if (lazy && dataPromises.size === 0) {
            readyPromise = createDeferredPromise();
            subscriptionValue.ready = readyPromise.promise;

            await redisUnsubscribe({
              channel: name,
              pattern: false,
            });
            return;
          }
        }

        dataPromises.add(dataPromise);

        const subscribing = redisSubscribe({
          channel: name,
          pattern: false,
        })?.then(readyPromise.resolve, readyPromise.reject);

        if (subscribing) await subscribing;

        while (true) {
          await dataPromise.current.promise;

          for (const value of dataPromise.current.values as Output[]) yield value;

          if (dataPromise.current.isDone) {
            break;
          } else {
            dataPromise.current = pubsubDeferredPromise();
          }
        }

        await dataPromise.unsubscribe();
      },
      async publish(...values: [Output, ...Output[]]) {
        await Promise.all(
          values.map(async (value) => {
            let parsedValue: Output;

            try {
              parsedValue = await schema.parseAsync(value);
            } catch (err) {
              onParseError(err);
              return;
            }

            await publisher.publish(name, stringify(parsedValue));
          })
        );
      },
      async unsubscribeAll() {
        await Promise.all(Array.from(dataPromises.values()).map((v) => v.unsubscribe()));
        await redisUnsubscribe({
          channel: name,
          pattern: false,
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
