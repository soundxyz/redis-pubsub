import assert from "assert";
import test, { type ExecutionContext } from "ava";
import Redis from "ioredis";
import { stringify } from "superjson";
import waitForExpect from "wait-for-expect";
import { z, ZodError } from "zod";
import type { RedisPubSubOptions } from "../src";
import { createDeferredPromise } from "../src/promise";
import { getPubsub } from "./helpers";

function baseTest(options?: Partial<RedisPubSubOptions>) {
  return async (t: ExecutionContext<unknown>) => {
    const pubSub = getPubsub(options);

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

    await channel.isReady();

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
  };
}

test("subscribe/unsubscribe and abort controller", baseTest());

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

  await channel.isReady();

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

  const subscribeError = await channel.isReady().then(
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

  await channel.isReady();

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

  await Promise.all([channel.unsubscribeAll(), channel.isReady()]);

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

  await channel.isReady();

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

  await channel.isReady();
  await channel.publish({
    value: "hello world",
  });

  t.deepEqual(await messagePromise.promise, {
    channel: "test",
    message: stringify("hello world"),
  });
});

test("subscribe and publish with specifiers", async (t) => {
  const pubSub = getPubsub();

  t.teardown(pubSub.close);

  const channel = pubSub.createChannel({
    name: "test-specifier:",
    schema: z.string(),
  });

  const subscription1 = (async () => {
    for await (const data of channel.subscribe({
      identifier: 1,
    })) {
      return data;
    }
  })();

  const subscription2 = (async () => {
    for await (const data of channel.subscribe({
      identifier: 2,
    })) {
      return data;
    }
  })();

  await channel.isReady(
    {
      identifier: 1,
    },
    {
      identifier: 2,
    }
  );

  await channel.publish(
    {
      identifier: 1,
      value: "1",
    },
    {
      identifier: 2,
      value: "2",
    }
  );

  t.deepEqual(await Promise.all([subscription1, subscription2]), ["1", "2"]);
});

test("unsubscribe specific channels", async (t) => {
  const { close, createChannel } = getPubsub();

  t.teardown(close);

  const channel = createChannel({
    name: "test",
    schema: z.string(),
  });

  const subscription1 = (async () => {
    for await (const value of channel.subscribe()) {
      return value;
    }
  })();

  const subscription2 = (async () => {
    for await (const value of channel.subscribe({
      identifier: 1,
    })) {
      return value;
    }
  })();

  await channel.isReady(
    {},
    {
      identifier: 1,
    }
  );

  await channel.unsubscribe(
    {},
    {
      identifier: 1,
    },
    {
      identifier: 2,
    }
  );

  await channel.publish(
    {
      value: "Unexpected value",
    },
    {
      value: "Unexpected value",
      identifier: 1,
    },
    {
      value: "Unexpected value",
      identifier: 2,
    }
  );

  t.deepEqual(await Promise.all([subscription1, subscription2]), [undefined, undefined]);
});

test("separate input and output schema", async (t) => {
  const pubSub = getPubsub();

  t.teardown(pubSub.close);

  class CustomClass {
    constructor(public name: string) {}
  }

  const inputSchema = z.string();
  const outputSchema = z.string().transform((input) => new CustomClass(input));

  const channel = pubSub.createChannel({
    name: "separate-type",
    inputSchema,
    outputSchema,
  });

  const subscription = (async () => {
    for await (const data of channel.subscribe()) {
      t.true(data instanceof CustomClass);
      t.is(data.name, "test");
      return data;
    }
  })();

  await channel.isReady();

  await channel.publish({
    value: "test",
  });

  const result = await subscription;

  t.true(result instanceof CustomClass);
  assert(result);
  t.is(result.name, "test");
});

test(
  "logLevel=silent subscribe/unsubscribe and abort controller",
  baseTest({
    logEvents: undefined,
  })
);

test(
  "logLevel=info subscribe/unsubscribe and abort controller",
  baseTest({
    logEvents: {
      events: {
        PUBLISH_MESSAGE: "PUBLISH",
        SUBSCRIBE_REDIS: false,
        SUBSCRIBE_EXECUTION_TIME: console.log,
      },
    },
  })
);
