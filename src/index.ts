import type { Redis } from "ioredis";
import { parse, stringify } from "superjson";
import type { ZodSchema, ZodTypeDef } from "zod";
import {
  createDeferredPromise,
  type DeferredPromise,
  type PubSubDeferredPromise,
  pubsubDeferredPromise,
} from "./promise";

export type EventParamsObject = Record<string, string | number | boolean | null | undefined>;

export type LogEventArgs = { message: string; code: EventCodes; params: EventParamsObject };

export type LoggedEvents = Partial<
  Record<EventCodes, string | boolean | null | ((args: LogEventArgs) => void)>
>;

function defaultLog({ message }: LogEventArgs) {
  console.log(message);
}

export interface RedisPubSubOptions {
  publisher: Redis;
  subscriber: Redis;
  onParseError?: (err: unknown) => void;
  /**
   * Enable and customize observability
   */
  logEvents?: {
    /**
     * Set specific events to enable logging
     */
    events: LoggedEvents;

    /**
     * @default console.log
     */
    log?: (args: LogEventArgs) => void;
  };
}

export const EventCodes = {
  SUBSCRIPTION_MESSAGE_WITHOUT_SUBSCRIBERS: "SUBSCRIPTION_MESSAGE_WITHOUT_SUBSCRIBERS",
  SUBSCRIPTION_MESSAGE_WITH_SUBSCRIBERS: "SUBSCRIPTION_MESSAGE_WITH_SUBSCRIBERS",
  SUBSCRIPTION_MESSAGE_EXECUTION_TIME: "SUBSCRIPTION_MESSAGE_EXECUTION_TIME",
  SUBSCRIPTION_MESSAGE_FILTERED_OUT: "SUBSCRIPTION_MESSAGE_FILTERED_OUT",
  SUBSCRIBE_REDIS: "SUBSCRIBE_REDIS",
  UNSUBSCRIBE_REDIS: "UNSUBSCRIBE_REDIS",
  UNSUBSCRIBE_CHANNEL: "UNSUBSCRIBE_CHANNEL",
  SUBSCRIBE_REUSE_REDIS_SUBSCRIPTION: "SUBSCRIBE_REUSE_REDIS_SUBSCRIPTION",
  SUBSCRIBE_EXECUTION_TIME: "SUBSCRIBE_EXECUTION_TIME",
  UNSUBSCRIBE_EXECUTION_TIME: "UNSUBSCRIBE_EXECUTION_TIME",
  SUBSCRIPTION_FINISHED: "SUBSCRIPTION_FINISHED",
  PUBLISH_MESSAGE: "PUBLISH_MESSAGE",
  PUBLISH_MESSAGE_EXECUTION_TIME: "PUBLISH_MESSAGE_EXECUTION_TIME",
  SUBSCRIPTION_ABORTED: "SUBSCRIPTION_ABORTED",
} as const;

export type EventCodes = (typeof EventCodes)[keyof typeof EventCodes];

export function RedisPubSub({
  publisher,
  subscriber,
  logEvents,
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

  const enabledLogEvents = logEvents?.events;

  const logMessage = logEvents
    ? function logMessage(code: EventCodes, params: EventParamsObject) {
        const eventValue = logEvents.events[code];

        if (!eventValue) return;

        const log = typeof eventValue === "function" ? eventValue : logEvents.log || defaultLog;

        const codeMessageValue = typeof eventValue === "string" ? eventValue : code;

        let paramsString = "";

        for (const key in params) {
          let value = params[key];

          if (value === undefined) continue;

          if (value === "") value = "null";

          paramsString += " " + key + "=" + value;
        }

        log({
          code,
          message: `[${codeMessageValue}]${paramsString}`,
          params,
        });
      }
    : () => void 0;

  return {
    createChannel,
    unsubscribeAll,
    close,
  };

  function getTracing() {
    const start = performance.now();

    return () => `${(performance.now() - start).toFixed()}ms`;
  }

  async function onMessage(channel: string, message: string) {
    const tracing = enabledLogEvents?.SUBSCRIPTION_MESSAGE_EXECUTION_TIME ? getTracing() : null;

    const subscription = subscriptionsMap[channel];

    if (!subscription?.dataPromises.size) {
      if (enabledLogEvents?.SUBSCRIPTION_MESSAGE_WITHOUT_SUBSCRIBERS) {
        logMessage("SUBSCRIPTION_MESSAGE_WITHOUT_SUBSCRIBERS", { channel });
      }
      return;
    }

    let parsedMessage: unknown;

    try {
      parsedMessage = await subscription.outputSchema.parseAsync(parse(message));
    } catch (err) {
      onParseError(err);
      return;
    }

    if (enabledLogEvents?.SUBSCRIPTION_MESSAGE_WITH_SUBSCRIBERS) {
      logMessage("SUBSCRIPTION_MESSAGE_WITH_SUBSCRIBERS", {
        channel,
        subscribers: subscription.dataPromises.size,
      });
    }

    for (const dataPromise of subscription.dataPromises) {
      dataPromise.current.values.push(parsedMessage);
      dataPromise.current.resolve();
    }

    if (tracing) {
      logMessage("SUBSCRIPTION_MESSAGE_EXECUTION_TIME", {
        channel,
        time: tracing(),
        subscribers: subscription.dataPromises.size,
      });
    }
  }

  function redisSubscribe({ channel }: { channel: string }) {
    const subscribed = subscribedChannels[channel];

    if (subscribed) {
      if (typeof subscribed === "boolean") {
        if (enabledLogEvents?.SUBSCRIBE_REUSE_REDIS_SUBSCRIPTION) {
          logMessage("SUBSCRIBE_REUSE_REDIS_SUBSCRIPTION", {
            channel,
          });
        }
        return;
      }

      return subscribed;
    }

    const tracing = enabledLogEvents?.SUBSCRIBE_EXECUTION_TIME ? getTracing() : null;

    return (subscribedChannels[channel] = subscriber.subscribe(channel).then(
      () => {
        subscribedChannels[channel] = true;

        if (enabledLogEvents?.SUBSCRIBE_REDIS) {
          logMessage("SUBSCRIBE_REDIS", {
            channel,
          });
        }

        if (tracing) {
          logMessage("SUBSCRIBE_EXECUTION_TIME", {
            channel,
            time: tracing(),
          });
        }
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
      const tracing = enabledLogEvents?.UNSUBSCRIBE_EXECUTION_TIME ? getTracing() : null;

      return (unsubscribingChannels[channel] = subscriber.unsubscribe(channel).then(
        () => {
          unsubscribingChannels[channel] = subscribedChannels[channel] = false;

          if (enabledLogEvents?.UNSUBSCRIBE_REDIS) {
            logMessage("UNSUBSCRIBE_REDIS", {
              channel,
            });
          }

          if (tracing) {
            logMessage("UNSUBSCRIBE_EXECUTION_TIME", {
              channel,
              time: tracing(),
            });
          }
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

  type Maybe<T> = T | null | undefined;

  function createChannel<PublishInput, ChannelData, SubscriberData>({
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
        inputSchema: ZodSchema<Maybe<ChannelData>, ZodTypeDef, PublishInput>;
        outputSchema: ZodSchema<SubscriberData, ZodTypeDef, NonNullable<ChannelData>>;
        schema?: never;
      }
    | {
        schema: ZodSchema<NonNullable<SubscriberData>, ZodTypeDef, Maybe<PublishInput>>;
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

    function subscribe<FilteredValue extends SubscriberData>(subscribeArguments: {
      abortSignal?: AbortSignal;
      filter: (value: SubscriberData) => value is FilteredValue;
      identifier?: string | number;
    }): AsyncGenerator<FilteredValue, void, unknown>;
    function subscribe(subscribeArguments?: {
      abortSignal?: AbortSignal;
      filter?: (value: SubscriberData) => unknown;
      identifier?: string | number;
    }): AsyncGenerator<SubscriberData, void, unknown>;
    async function* subscribe({
      abortSignal,
      filter,
      identifier,
    }: {
      abortSignal?: AbortSignal;
      filter?: (value: SubscriberData) => unknown;
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
            if (enabledLogEvents?.SUBSCRIPTION_ABORTED) {
              logMessage("SUBSCRIPTION_ABORTED", {
                channel,
                subscribers: dataPromises.size,
              });
            }
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

        if (enabledLogEvents?.UNSUBSCRIBE_CHANNEL) {
          logMessage("UNSUBSCRIBE_CHANNEL", {
            channel,
            subscribers: dataPromises.size,
          });
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

      try {
        const subscribing = redisSubscribe({
          channel,
        })?.then(subscriptionValue.ready.resolve, subscriptionValue.ready.reject);

        if (subscribing) await subscribing;

        while (true) {
          await dataPromise.current.promise;

          for (const value of dataPromise.current.values as SubscriberData[]) {
            if (filter && !filter(value)) {
              if (enabledLogEvents?.SUBSCRIPTION_MESSAGE_FILTERED_OUT) {
                logMessage("SUBSCRIPTION_MESSAGE_FILTERED_OUT", {
                  channel,
                });
              }
              continue;
            }

            yield value;
          }

          if (dataPromise.current.isDone) {
            if (enabledLogEvents?.SUBSCRIPTION_FINISHED) {
              logMessage("SUBSCRIPTION_FINISHED", {
                channel,
              });
            }
            break;
          } else {
            dataPromise.current = pubsubDeferredPromise();
          }
        }
      } finally {
        await dataPromise.unsubscribe();
      }
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
        { value: Maybe<PublishInput>; identifier?: string | number },
        ...{ value: Maybe<PublishInput>; identifier?: string | number }[],
      ]
    ) {
      await Promise.all(
        values.map(async ({ value, identifier }) => {
          if (value == null) return;

          const tracing = enabledLogEvents?.PUBLISH_MESSAGE_EXECUTION_TIME ? getTracing() : null;

          let parsedValue: Maybe<ChannelData | SubscriberData>;

          try {
            parsedValue = await inputSchema.parseAsync(value);
            if (parsedValue == null) return;
          } catch (err) {
            onParseError(err);
            return;
          }

          const channel = identifier ? name + identifier : name;

          await publisher.publish(channel, stringify(parsedValue));

          if (enabledLogEvents?.PUBLISH_MESSAGE) {
            logMessage("PUBLISH_MESSAGE", {
              channel,
            });
          }

          if (tracing) {
            logMessage("PUBLISH_MESSAGE_EXECUTION_TIME", {
              channel,
              time: tracing(),
            });
          }
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

export function subscription<T>(
  asyncGenerator: (args: {
    abortSignal: AbortSignal;
    abortController: AbortController;
  }) => AsyncGenerator<T>
) {
  const abortController = new AbortController();

  const asyncIterator = asyncGenerator({
    abortController,
    abortSignal: abortController.signal,
  });

  const asyncReturn = asyncIterator.return;

  asyncIterator.return = () => {
    abortController.abort();

    return asyncReturn.call(asyncIterator, undefined);
  };

  return asyncIterator;
}
