import type { Redis } from "ioredis";
import { parse, stringify } from "superjson";
import type { ZodSchema, ZodTypeDef } from "zod";
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
  interface DataPromise {
    current: PubSubDeferredPromise<unknown>;
    unsubscribe: () => Promise<void>;
  }
  interface SubscriptionValue {
    readonly name: string;
    readonly identifier: string | undefined;
    readonly channel: string;
    readonly inputSchema: ZodSchema<unknown>;
    readonly outputSchema: ZodSchema<unknown>;
    readonly dataPromises: Set<DataPromise>;
    ready: DeferredPromise<void>;
  }
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
      parsedMessage = await subscription.outputSchema.parseAsync(parse(message));
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

  function createChannel<Input, Output>({
    name,
    isLazy = true,
    ...schemas
  }: {
    name: string;
    /**
     * @default true
     */
    isLazy?: boolean;
  } & (
    | {
        inputSchema: ZodSchema<Input, ZodTypeDef, Input>;
        outputSchema: ZodSchema<Output, ZodTypeDef, Input>;
        schema?: never;
      }
    | {
        schema: ZodSchema<Output, ZodTypeDef, Input>;
        inputSchema?: never;
        outputSchema?: never;
      }
  )) {
    const { inputSchema, outputSchema } =
      "schema" in schemas && schemas.schema
        ? { inputSchema: schemas.schema, outputSchema: schemas.schema }
        : schemas;

    if (!isLazy) {
      const channel = name;
      const initialSubscriptionValue = getSubscriptionValue({
        name,
        channel,
        identifier: undefined,
      });
      redisSubscribe({
        channel,
      })?.then(initialSubscriptionValue.ready.resolve, initialSubscriptionValue.ready.reject);
    }

    return {
      isReady,
      subscribe,
      unsubscribe,
      publish,
      unsubscribeAll,
      inputSchema,
      outputSchema,
    };

    function getSubscriptionValue({
      name,
      channel,
      identifier,
    }: {
      name: string;
      channel: string;
      identifier: string | number | undefined;
    }) {
      return (subscriptionsMap[channel] ||= {
        dataPromises: new Set<DataPromise>(),
        name,
        channel,
        identifier: identifier?.toString(),
        inputSchema,
        outputSchema,
        ready: createDeferredPromise(),
      });
    }

    function subscribe<FilteredValue extends Output>(subscribeArguments: {
      abortSignal?: AbortSignal;
      filter: (value: Output) => value is FilteredValue;
      identifier?: string | number;
    }): AsyncGenerator<FilteredValue, void, unknown>;
    function subscribe(subscribeArguments?: {
      abortSignal?: AbortSignal;
      filter?: (value: Output) => unknown;
      identifier?: string | number;
    }): AsyncGenerator<Output, void, unknown>;
    async function* subscribe({
      abortSignal,
      filter,
      identifier,
    }: {
      abortSignal?: AbortSignal;
      filter?: (value: Output) => unknown;
      identifier?: string | number;
    } = {}) {
      const channel = identifier ? name + identifier : name;

      const subscriptionValue = getSubscriptionValue({
        name,
        identifier,
        channel,
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

        for (const value of dataPromise.current.values as Output[]) {
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

    async function unsubscribe(
      channel?: { identifier?: string | number },
      ...channels: Array<{ identifier?: string | number } | undefined>
    ) {
      await Promise.all(
        [channel, ...channels].flatMap(({ identifier } = {}) => {
          const channel = identifier ? name + identifier : name;

          const subscriptionValue = subscriptionsMap[channel];

          if (!subscriptionValue?.dataPromises.size) return;

          return [
            ...Array.from(subscriptionValue.dataPromises).map((v) => v.unsubscribe()),
            redisUnsubscribe({
              channel,
            }),
          ];
        })
      );
    }

    async function isReady(
      channel?: { identifier?: string | number },
      ...channels: Array<{ identifier?: string | number } | undefined>
    ) {
      await Promise.all(
        [channel, ...channels].map(({ identifier } = {}) => {
          const channel = identifier ? name + identifier : name;

          return getSubscriptionValue({
            name,
            channel,
            identifier,
          }).ready.promise;
        })
      );
    }

    async function publish(
      ...values: [
        { value: Input; identifier?: string | number },
        ...{ value: Input; identifier?: string | number }[]
      ]
    ) {
      await Promise.all(
        values.map(async ({ value, identifier }) => {
          let parsedValue: Input | Output;

          try {
            parsedValue = await inputSchema.parseAsync(value);
          } catch (err) {
            onParseError(err);
            return;
          }

          await publisher.publish(identifier ? name + identifier : name, stringify(parsedValue));
        })
      );
    }

    async function unsubscribeAll() {
      const subscriptions = Object.values(subscriptionsMap).filter((value) => value.name === name);

      await Promise.all(
        subscriptions.flatMap(({ dataPromises, channel }) => [
          ...Array.from(dataPromises).map(({ unsubscribe }) => unsubscribe()),
          redisUnsubscribe({
            channel,
          }),
        ])
      );
    }
  }

  async function close() {
    subscriber.off("message", onMessage);

    await unsubscribeAll();

    subscriber.disconnect();
    publisher.disconnect();
  }
}
