import test from "ava";
import { CreateTestClient } from "@graphql-ez/fastify-testing";
import SchemaBuilder from "@pothos/core";
import { ezWebSockets } from "@graphql-ez/plugin-websockets";
import { getPubsub } from "./helpers";
import { z } from "zod";
import { subscription } from "../src";
import { createDeferredPromise } from "../src/promise";

test("graphql subscription", async (t) => {
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

  const backendCleanedPromise = createDeferredPromise(2000);

  const backendDataPromise = createDeferredPromise<number>(2000);

  builder.subscriptionType({
    fields(t) {
      return {
        test: t.int({
          subscribe() {
            return subscription(async function* ({ abortSignal }) {
              try {
                for await (const data of channel.subscribe({
                  abortSignal,
                })) {
                  yield data;
                  backendDataPromise.resolve(data);
                }
              } finally {
                backendCleanedPromise.resolve();
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
  const { assertedQuery, websockets, cleanup } = await CreateTestClient(
    {
      schema: builder.toSchema({}),
      ez: {
        plugins: [ezWebSockets("new")],
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

  t.teardown(cleanup);

  const { __typename } = await assertedQuery("{__typename}");

  t.is(__typename, "Query");

  const { iterator, unsubscribe } = websockets.subscribe<{ test: number }>("subscription{test}");

  await channel.isReady();

  await channel.publish({ value: 1 });

  let iteratorData: number | undefined;

  for await (const { data } of iterator) {
    iteratorData = data?.test;

    unsubscribe().catch(t.fail);
  }

  t.is(iteratorData, 1);

  await backendCleanedPromise.promise;

  t.is(await backendDataPromise.promise, 1);
});
