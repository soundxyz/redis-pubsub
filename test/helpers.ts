import { RedisPubSub, RedisPubSubOptions } from "../src";
import Pino from "pino";
import Redis from "ioredis";

export const getPubsub = (options?: Partial<RedisPubSubOptions>) => {
  const logger = Pino({
    level: process.env.CI ? "warn" : "info",
  });

  const publisher = new Redis({
    port: 6389,
  });
  const subscriber = new Redis({
    port: 6389,
  });

  const pubSub = RedisPubSub({
    publisher,
    subscriber,
    logger,
    logLevel: "tracing",
    ...options,
  });

  return {
    ...pubSub,
    publisher,
    subscriber,
  };
};
