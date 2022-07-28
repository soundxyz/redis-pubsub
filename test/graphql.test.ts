import test from "ava";
import { CreateTestClient } from "@graphql-ez/fastify-testing";
import SchemaBuilder from "@pothos/core";
import { ezWebSockets } from "@graphql-ez/plugin-websockets";
import { getPubsub } from "./helpers";
import { z } from "zod";
import * as timers from "timers/promises";
test("hello", async (t) => {
  const pubsub = getPubsub();
  const builder = new SchemaBuilder({});

  const channel = pubsub.createChannel({
    name: "test",
    schema: z.number(),
  });

  builder.queryType({
    fields(t) {
      return {
        hello: t.boolean({
          resolve() {
            return true;
          },
        }),
      };
    },
  });

  const withCancel = <T>(
    asyncGenerator: (args: { abortSignal: AbortSignal }) => AsyncGenerator<T>
  ) => {
    const abortController = new AbortController();

    const asyncIterator = asyncGenerator({
      abortSignal: abortController.signal,
    });
    const asyncReturn = asyncIterator.return;

    asyncIterator.return = () => {
      console.log("cancel", asyncReturn);
      abortController.abort();

      return asyncReturn
        ? asyncReturn.call(asyncIterator, undefined)
        : Promise.resolve({ value: undefined, done: true });
    };

    return asyncIterator;
  };

  builder.subscriptionType({
    fields(t) {
      return {
        test: t.int({
          subscribe() {
            return withCancel(async function* subscribe({ abortSignal }) {
              try {
                for await (const data of channel.subscribe({
                  abortSignal,
                })) {
                  console.log({ data });
                  yield data;
                }
              } finally {
                console.log("cleaned");
              }
            });
          },
          resolve(t) {
            return t;
          },
        }),
      };
    },
  });
  const { assertedQuery, websockets } = await CreateTestClient(
    {
      schema: builder.toSchema({}),
      ez: {
        plugins: [
          ezWebSockets({
            graphQLWS: {
              onComplete() {
                console.log("on complete");
              },
              onNext() {
                console.log("on next");
              },
            },
          }),
        ],
      },
    },
    {
      clientOptions: {
        graphQLWSClientOptions: {
          lazy: false,
        },
      },
    }
  );

  const { __typename } = await assertedQuery("{__typename}");

  console.log({
    __typename,
  });

  t.true(true);

  const wsClient = await websockets.client;

  const asd = wsClient.subscribe(
    {
      query: "subscription{test}",
    },
    {
      complete() {
        console.log("complete");
      },
      error(err) {
        console.log("error", err);
      },
      next(value) {
        console.log("Next", value);
      },
    }
  );

  // const subscribe = websockets.subscribe("subscription{test}");

  await channel.isReady();

  await timers.setTimeout(100);

  await channel.publish({ value: 1 });

  await timers.setTimeout(100);

  await asd();
  // setTimeout(() => {
  //   subscribe.unsubscribe();
  // }, 50);

  // for await (const data of subscribe.iterator) {
  //   console.log({
  //     data,
  //   });
  // }

  // await timers.setTimeout(100);

  // await subscribe.unsubscribe();

  // const client = await websockets.client;

  // await client.dispose();

  await timers.setTimeout(2000);
});
