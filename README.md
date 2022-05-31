# redis-pubsub

Full type-safe Redis PubSub system with async iterators

## Features

- [x] Type-safety with [Zod](https://github.com/colinhacks/zod)
- [x] Out-of-the-box support for `Date`/`Map`/`Set`/`BigInt` serialization with [superjson](https://github.com/blitz-js/superjson)
- [x] Full usage of Async Iterators
- [x] Support for [AbortController](https://developer.mozilla.org/en-US/docs/Web/API/AbortController) / [AbortSignal](https://developer.mozilla.org/en-US/docs/Web/API/AbortSignal)
- [x] Support for type-safe filtering using [Type Guards / Type predicates](https://www.typescriptlang.org/docs/handbook/2/narrowing.html#using-type-predicates)
- [x] Support for **Entity** + **Identifier** pattern subscriptions
- [x] Support for Async [Zod Refines](https://zod.dev/?id=refine) and Async [Zod Transforms](https://zod.dev/?id=transform)
- [x] [GraphQL](https://graphql.org/) API ready

## Install

```sh
pnpm add @soundxyz/redis-pubsub
```

```sh
npm install @soundxyz/redis-pubsub
```

```sh
yarn add @soundxyz/redis-pubsub
```

> Peer dependencies

```sh
pnpm add zod ioredis
```

```sh
npm install zod ioredis
```

```sh
yarn add zod ioredis
```

## Usage

### Create a Redis PubSub instance:

```ts
import Redis from "ioredis";
import { z } from "zod";

import { RedisPubSub } from "@soundxyz/redis-pubsub";

const { createChannel } = RedisPubSub({
  publisher: new Redis({
    port: 6379,
  }),
  subscriber: new Redis({
    port: 6379,
  }),
});
```

### Create a channel with any `Zod` schema and a unique `"name"` to be used as main trigger.

```ts
const schema = z.object({
  id: z.string(),
  name: z.string(),
});

const userChannel = createChannel({
  name: "User",
  schema,
});

const nonLazyUserChannel = createChannel({
  name: "User",
  schema,
  // By default the channels are lazily connected with redis
  isLazy: false,
});
```

### Subscribe and publish to the channel

```ts
// Using async iterators / async generators to subscribe
(async () => {
  for await (const user of userChannel.subscribe()) {
    console.log("User", {
      id: user.id,
      name: user.name,
    });
  }
})();

// You can explicitly wait until the channel is sucessfully connected with Redis
await userChannel.isReady();

// Publish data into the channel
await userChannel.publish(
  {
    value: {
      id: "1",
      name: "John",
    },
  },
  // You can also publish more than a single value
  {
    value: {
      id: "2",
      name: "Peter",
    },
  }
);
```

### Filter based on the data

```ts
(async () => {
  for await (const user of userChannel.subscribe({
    filter(value) {
      return value.id === "1";
    },
  })) {
    console.log("User 1", {
      id: user.id,
      name: user.name,
    });
  }
})();

// You can also use type predicates / type guards
(async () => {
  for await (const user of userChannel.subscribe({
    filter(value): value is { id: "1"; name: string } {
      return value.id === "1";
    },
  })) {
    // typeof user.id == "1"
    console.log("User 1", {
      id: user.id,
      name: user.name,
    });
  }
})();
```

### Use custom identifiers

It will create a separate redis channel for every identifier, concatenating `"name"` and `"identifier"`, for example, with `"name"`=`"User"` and `"identifier"` = `1`, the channel trigger name will be `"User1"`

```ts
(async () => {
  for await (const user of userChannel.subscribe({
    // number or string
    identifier: 1,
  })) {
    console.log("User with identifier=1", {
      id: user.id,
      name: user.name,
    });
  }
})();

await userChannel.isReady({
  // number or string
  identifier: 1,
});

await userChannel.publish({
  value: {
    id: "1",
    name: "John",
  },
  identifier: 1,
});
```

### Separate input from output

You can levarage [Zod Transforms](https://zod.dev/?id=transform) to be able to separate input types from the output types, and receive any custom class or output on your subscriptions.

```ts
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
    return data;
  }
})();

await channel.isReady();

await channel.publish({
  value: "test",
});

const result = await subscription;

// true
console.log(result instanceof CustomClass);

// true
console.log(result.name === "test");
```

### Use AbortController / AbortSignal

If `isLazy` is **not** disabled, the last subscription to a channel will be automatically unsubscribed from **Redis**.

```ts
const abortController = new AbortController();
const abortedSubscription = (() => {
  for await (const data of userChannel.subscribe({
    abortSignal: abortController.signal,
  })) {
    console.log({ data });
  }
})();

// ...

firstSubscribeAbortController.abort();

await abortedSubscription;
```

### Unsubscribe specific identifiers

```ts
await userChannel.unsubscribe(
  {
    identifier: 1,
  },
  // You can specify more than a single identifer at once
  {
    identifier: 2,
  }
);
```

### Unsubscribe an entire channel

```ts
await userChannel.unsubscribeAll();
```

### Close the PubSub instance

```ts
const pubSub = RedisPubSub({
  publisher: new Redis({
    port: 6379,
  }),
  subscriber: new Redis({
    port: 6379,
  }),
});

// ...
await pubSub.close();
```
