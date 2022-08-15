import { LoggedEvents, RedisPubSub, RedisPubSubOptions } from "../src";
import Pino from "pino";
import Redis from "ioredis";

const logEverything: Required<LoggedEvents> = {
  PUBLISH_MESSAGE: true,
  PUBLISH_MESSAGE_EXECUTION_TIME: true,
  SUBSCRIBE_EXECUTION_TIME: true,
  SUBSCRIBE_REDIS: true,
  SUBSCRIBE_REUSE_REDIS_SUBSCRIPTION: true,
  SUBSCRIPTION_ABORTED: true,
  SUBSCRIPTION_FINISHED: true,
  SUBSCRIPTION_MESSAGE_EXECUTION_TIME: true,
  SUBSCRIPTION_MESSAGE_FILTERED_OUT: true,
  SUBSCRIPTION_MESSAGE_WITH_SUBSCRIBERS: true,
  SUBSCRIPTION_MESSAGE_WITHOUT_SUBSCRIBERS: true,
  UNSUBSCRIBE_CHANNEL: true,
  UNSUBSCRIBE_EXECUTION_TIME: true,
  UNSUBSCRIBE_REDIS: true,
};

export const logger = Pino({
  level: process.env.CI ? "warn" : "info",
});

export const getPubsub = (options?: Partial<RedisPubSubOptions>) => {
  const publisher = new Redis({
    port: 6389,
  });
  const subscriber = new Redis({
    port: 6389,
  });

  const pubSub = RedisPubSub({
    publisher,
    subscriber,
    logEvents: {
      events: logEverything,
      log({ message }) {
        logger.info(message);
      },
    },
    ...options,
  });

  return {
    ...pubSub,
    publisher,
    subscriber,
  };
};
