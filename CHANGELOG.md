# @soundxyz/redis-pubsub

## 4.1.1

### Patch Changes

- ca6952f: Allow null or undefined inputs on schema level

## 4.1.0

### Minor Changes

- 69f7422: .publish can receive null or undefined and it skips the notification

## 4.0.0

### Major Changes

- ff78cf7: Separate input from output of inputSchema and outputSchema
- 37411c1: Make superjson peer dependency

## 3.0.1

### Patch Changes

- 795b08e: Fix SUBSCRIPTION_MESSAGE_WITH_SUBSCRIBERS log event

## 3.0.0

### Major Changes

- 7cecccb: Rework observability into logEvents.events + logEvents.log for full customizability

## 2.2.1

### Patch Changes

- 4574ae0: Peer dependencies as optional

## 2.2.0

### Minor Changes

- 33c49d4: New "customizeEventCodes" to be able to change or disable specified events logs

### Patch Changes

- 33c49d4: Remove all decimals from tracing values

## 2.1.0

### Minor Changes

- 17a8cf7: New "subscription" helper exported from library with abortSignal and abortController, specially useful for GraphQL Usage

### Patch Changes

- 7dcf49a: Add try-finally block to subscribe generator

## 2.0.0

### Major Changes

- 748ce12: New Pino logger required option + optional "logLevel" option (default is "silent") to debug and trace subscriptions performance

### Patch Changes

- 2623d83: Fix wildcard imports

## 1.1.1

### Patch Changes

- 9b23a1f: Give back inputSchema and outputSchema on createChannel for re-use

## 1.1.0

### Minor Changes

- 87cf525: Allow separate input from output zod schemas

## 1.0.2

### Patch Changes

- 3546795: Fix cjs publish

## 1.0.1

### Patch Changes

- b421b1e: Fix publish to only ship dist

## 1.0.0

### Major Changes

- 33d1ce7: Release
