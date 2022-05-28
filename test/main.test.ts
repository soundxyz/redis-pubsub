import test from "ava";
import Redis from "ioredis";
import waitForExpect from "wait-for-expect";
import { z } from "zod";
import { RedisPubSub } from "../src";

test("subscribe/unsubscribe and abort controller", async (t) => {
  const pubSub = RedisPubSub({
    publisher: new Redis({
      port: 6389,
    }),
    subscriber: new Redis({
      port: 6389,
    }),
  });

  const channel = pubSub.createChannel({
    schema: z.string(),
    name: "test",
  });

  const firstSubscribeAbortController = new AbortController();

  const firstSubscribeValues: string[] = [];
  const firstSubscribe = (async () => {
    for await (const data of channel.subscribe({
      abortSignal: firstSubscribeAbortController.signal,
    })) {
      firstSubscribeValues.push(data);
    }
  })();

  const secondSubscribeValues: string[] = [];
  const secondSubscribe = (async () => {
    for await (const data of channel.subscribe()) {
      secondSubscribeValues.push(data);
    }
  })();

  await channel.ready;

  await Promise.all([channel.publish("1", "2", "3")]);

  await waitForExpect(() => {
    t.deepEqual(firstSubscribeValues, ["1", "2", "3"]);
    t.deepEqual(secondSubscribeValues, ["1", "2", "3"]);
  });

  await Promise.all([channel.publish("4")]);

  await waitForExpect(() => {
    t.deepEqual(firstSubscribeValues, ["1", "2", "3", "4"]);
    t.deepEqual(secondSubscribeValues, ["1", "2", "3", "4"]);
  });

  firstSubscribeAbortController.abort();

  await firstSubscribe;

  await channel.publish("5");

  await waitForExpect(() => {
    t.deepEqual(firstSubscribeValues, ["1", "2", "3", "4"]);
    t.deepEqual(secondSubscribeValues, ["1", "2", "3", "4", "5"]);
  });

  t.deepEqual(firstSubscribeValues, ["1", "2", "3", "4"]);

  await channel.unsubscribeAll();

  await secondSubscribe;

  await channel.publish("6");

  t.deepEqual(firstSubscribeValues, ["1", "2", "3", "4"]);
  t.deepEqual(secondSubscribeValues, ["1", "2", "3", "4", "5"]);

  await pubSub.close();
});
