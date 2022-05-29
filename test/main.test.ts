import assert from "assert";
import test from "ava";
import Redis from "ioredis";
import { stringify } from "superjson";
import waitForExpect from "wait-for-expect";
import { z, ZodError } from "zod";
import { RedisPubSub, RedisPubSubOptions } from "../src";
import { createDeferredPromise } from "../src/promise";

const getPubsub = (options?: Partial<RedisPubSubOptions>) => {
  const publisher = new Redis({
    port: 6389,
  });
  const subscriber = new Redis({
    port: 6389,
  });

  const pubSub = RedisPubSub({
    publisher,
    subscriber,
    ...options,
  });

  return {
    ...pubSub,
    publisher,
    subscriber,
  };
};
test("subscribe/unsubscribe and abort controller", async (t) => {
  const pubSub = getPubsub();

  t.teardown(pubSub.close);

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

  await Promise.all([channel.publish({ value: "1" }, { value: "2" }, { value: "3" })]);

  await waitForExpect(() => {
    t.deepEqual(firstSubscribeValues, ["1", "2", "3"]);
    t.deepEqual(secondSubscribeValues, ["1", "2", "3"]);
  });

  await Promise.all([channel.publish({ value: "4" })]);

  await waitForExpect(() => {
    t.deepEqual(firstSubscribeValues, ["1", "2", "3", "4"]);
    t.deepEqual(secondSubscribeValues, ["1", "2", "3", "4"]);
  });

  firstSubscribeAbortController.abort();

  await firstSubscribe;

  await channel.publish({ value: "5" });

  await waitForExpect(() => {
    t.deepEqual(firstSubscribeValues, ["1", "2", "3", "4"]);
    t.deepEqual(secondSubscribeValues, ["1", "2", "3", "4", "5"]);
  });

  t.deepEqual(firstSubscribeValues, ["1", "2", "3", "4"]);

  await channel.unsubscribeAll();

  await secondSubscribe;

  await channel.publish({ value: "6" });

  t.deepEqual(firstSubscribeValues, ["1", "2", "3", "4"]);
  t.deepEqual(secondSubscribeValues, ["1", "2", "3", "4", "5"]);

  await pubSub.close();
});

test("parse error on publish", async (t) => {
  let receivedError!: unknown | undefined;
  function onParseError(err: unknown) {
    receivedError = err;
  }
  const pubSub = getPubsub({ onParseError });

  t.teardown(pubSub.close);

  const channel = pubSub.createChannel({
    schema: z.string(),
    name: "test",
  });

  await channel.publish({
    // @ts-expect-error
    value: 123,
  });

  t.assert(receivedError instanceof ZodError);
  assert(receivedError instanceof ZodError);

  t.deepEqual(receivedError.flatten().formErrors, ["Expected string, received number"]);

  await pubSub.close();
});

test("parse error on subscribe", async (t) => {
  let receivedError!: unknown | undefined;
  function onParseError(err: unknown) {
    receivedError = err;
  }

  const expectedMessagePromise = createDeferredPromise<string>(1000);
  const { createChannel, publisher, close } = getPubsub({ onParseError });

  t.teardown(close);

  const channel = createChannel({
    schema: z.string(),
    name: "test",
  });

  const subscription = (async () => {
    for await (const data of channel.subscribe()) {
      expectedMessagePromise.resolve(data);
    }
  })();

  await channel.ready;

  await publisher.publish("test", stringify(123));

  await publisher.publish("test", stringify("expected"));

  t.is(await expectedMessagePromise.promise, "expected");

  t.assert(receivedError instanceof ZodError);
  assert(receivedError instanceof ZodError);

  t.deepEqual(receivedError.flatten().formErrors, ["Expected string, received number"]);

  await close();

  await subscription;
});

test("internal publish error", async (t) => {
  const { createChannel, close } = getPubsub({
    publisher: {
      async publish(_channel: string, _message: string) {
        throw Error("Expected error");
      },
      disconnect() {},
    } as unknown as Redis,
  });

  t.teardown(close);

  const channel = createChannel({
    schema: z.string(),
    name: "test",
  });

  const publishError = await channel.publish({ value: "noop" }).then(
    () => Error("Unexpected error"),
    (err) => err
  );

  t.assert(publishError instanceof Error);
  assert(publishError instanceof Error);

  t.is(publishError.message, "Expected error");
});

test("internal subscribe error", async (t) => {
  const { createChannel, close } = getPubsub({
    subscriber: {
      async subscribe(_channel: string, _message: string) {
        throw Error("Expected error");
      },
      disconnect() {},
      on() {},
      off() {},
    } as unknown as Redis,
  });

  t.teardown(close);

  const channel = createChannel({
    schema: z.string(),
    name: "test",
    isLazy: false,
  });

  const subscribeError = await channel.ready.then(
    () => Error("Unexpected error"),
    (err) => err
  );

  t.assert(subscribeError instanceof Error);
  assert(subscribeError instanceof Error);

  t.is(subscribeError.message, "Expected error");
});

test("internal unsubscribe error", async (t) => {
  const { createChannel, publisher } = getPubsub({
    subscriber: {
      async subscribe(_channel: string, _message: string) {},
      async unsubscribe() {
        throw Error("Expected error");
      },
      disconnect() {},
      on() {},
      off() {},
    } as unknown as Redis,
  });

  t.teardown(() => {
    publisher.disconnect();
  });

  const channel = createChannel({
    schema: z.string(),
    name: "test",
    isLazy: false,
  });

  await channel.ready;

  const subscribeError = await channel.unsubscribeAll().then(
    () => Error("Unexpected error"),
    (err) => err
  );

  t.assert(subscribeError instanceof Error);
  assert(subscribeError instanceof Error);

  t.is(subscribeError.message, "Expected error");
});

test("unsubscribe while still subscribing", async (t) => {
  const subscriber = new Redis({
    port: 6389,
  });

  let didSubscribeEnd = false;

  const { close, createChannel } = getPubsub({
    subscriber: {
      async subscribe(...args) {
        return subscriber.subscribe(...(args as any[])).then(() => (didSubscribeEnd = true));
      },
      on(...args) {
        return subscriber.on(...args);
      },
      off(...args) {
        return subscriber.on(...args);
      },
      disconnect(...args) {
        return subscriber.disconnect(...args);
      },
      unsubscribe(...args) {
        t.is(didSubscribeEnd, true);
        return subscriber.unsubscribe(...(args as any[]));
      },
    } as Redis,
  });

  t.teardown(close);

  const channel = createChannel({
    name: "test",
    schema: z.string(),
    isLazy: false,
  });

  await Promise.all([channel.unsubscribeAll(), channel.ready]);

  t.is(didSubscribeEnd, true);
  await close();
});

test("filter works as expected", async (t) => {
  const { createChannel, close } = getPubsub();

  t.teardown(close);

  const channel = createChannel({
    name: "test",
    schema: z.number(),
    isLazy: false,
  });

  await channel.ready;

  const subscription1 = (async () => {
    for await (const value of channel.subscribe({
      filter(value): value is 1 {
        return value === 1;
      },
    })) {
      return value;
    }
  })();

  const subscription2 = (async () => {
    for await (const value of channel.subscribe({
      filter(value): value is 2 {
        return value === 2;
      },
    })) {
      return value;
    }
  })();

  await channel.publish({
    value: 1,
  });

  await channel.publish({
    value: 2,
  });

  t.is(await subscription1, 1);
  t.is(await subscription2, 2);
});

test("pattern with specifier works as expected", async (t) => {
  const firstMessageChannel = createDeferredPromise<string>(1000);
  const secondMessageChannel = createDeferredPromise<string>(1000);

  const { createChannel, close, subscriber } = getPubsub();

  t.teardown(close);

  let messageNumber = 0;
  const onPMessage = (pattern: string | undefined, channel: string, message: string) => {
    t.is(pattern, "test:*");
    switch (++messageNumber) {
      case 1:
        t.is(message, stringify(1));
        return firstMessageChannel.resolve(channel);
      case 2:
        t.is(message, stringify(2));
        return secondMessageChannel.resolve(channel);
      default:
        throw Error("Unexpected message");
    }
  };

  subscriber.on("pmessage", onPMessage);
  t.teardown(() => subscriber.off("pmessage", onPMessage));

  const channel = createChannel({
    name: "test:",
    subscriptionPattern: "test:*",
    schema: z.number(),
    isLazy: false,
  });

  const subscription = (async () => {
    const values: number[] = [];
    for await (const value of channel.subscribe()) {
      values.push(value);
      if (values.length === 2) return values;
    }
  })();

  await channel.ready;

  await channel.publish({
    value: 1,
    specifier: "1",
  });

  t.is(await firstMessageChannel.promise, "test:1");

  await channel.publish({
    value: 2,
    specifier: "2",
  });

  t.is(await secondMessageChannel.promise, "test:2");

  t.is(messageNumber, 2);

  t.deepEqual(await subscription, [1, 2]);

  await channel.unsubscribeAll();
});

test("publish without subscriptions", async (t) => {
  const { createChannel, close, subscriber } = getPubsub();

  t.teardown(close);

  const channel = createChannel({
    schema: z.string(),
    name: "test",
    isLazy: false,
  });

  const messagePromise = createDeferredPromise<{
    channel: string;
    message: string;
  }>(1000);

  function onMessage(channel: string, message: string) {
    messagePromise.resolve({ channel, message });
  }
  subscriber.on("message", onMessage);

  t.teardown(() => subscriber.off("message", onMessage));

  await channel.publish({
    value: "hello world",
  });

  t.deepEqual(await messagePromise.promise, {
    channel: "test",
    message: stringify("hello world"),
  });
});
