# @soundxyz/redis-pubsub

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
