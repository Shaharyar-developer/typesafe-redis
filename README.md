# typesafe-redis

> Type-safe, declarative Redis schema and client for TypeScript/Node.js

## Features

- **Type-safe Redis schema**: Define Redis keys, hashes, lists, sets, sorted sets, and JSON types with full TypeScript type safety.
- **Chainable API**: Compose constraints (e.g., optional, default, ttl, min/max) using a fluent builder pattern.
- **Schema-driven client**: Generate a Redis client with type-safe operations for your schema.
- **Supports all major Redis types**: String, Hash, List, Set, Sorted Set (ZSet), and JSON (with validation).
- **Custom validators**: Attach runtime type guards to JSON types for extra safety.
- **Automatic serialization**: Handles JSON and primitive serialization/deserialization for you.

## Installation

```sh
npm install typesafe-redis
# or
yarn add typesafe-redis
# or
bun add typesafe-redis
```

## Quick Example

```typescript
import {
  redisString,
  redisHash,
  redisHashString,
  redisHashNumber,
  redisJson,
  createRedisClient,
} from "typesafe-redis";

// Define your schema
const schema = {
  userName: redisString().optional().default("guest"),
  userProfile: redisJson<{ name: string; age: number }>().ttl(3600),
  userSettings: redisHash("user:settings", {
    theme: redisHashString().default("light"),
    notifications: redisHashNumber().default(1),
  }),
};

// Create a type-safe client
const client = createRedisClient("redis://localhost:6379", schema);

// Usage
await client.schema.userName.set("user:1:name", "Alice");
const name = await client.schema.userName.get("user:1:name"); // string | null

await client.schema.userProfile.set("user:1:profile", { name: "Alice", age: 30 });
const profile = await client.schema.userProfile.get("user:1:profile"); // { name: string; age: number } | null

await client.schema.userSettings.hset("user:settings:1", "theme", "dark");
const theme = await client.schema.userSettings.hget("user:settings:1", "theme"); // string | null
```

## API Overview

### Schema Types
- `redisString<T>()` — Redis string (default: string)
- `redisJson<T>()` — Redis JSON (with optional validator)
- `redisHash(key, fields)` — Redis hash with typed fields
- `redisList(key, elementType)` — Redis list
- `redisSet(key, elementType)` — Redis set
- `redisSortedSet(key, elementType)` — Redis sorted set (zset)

### Chainable Constraints
- `.optional()` — Mark as optional
- `.default(value)` — Set a default value
- `.ttl(seconds)` — Set time-to-live
- `.description(text)` — Add a description
- `.minLength(n)`, `.maxLength(n)` — For strings/lists
- `.maxSize(n)` — For sets/zsets
- `.schema(validator)` — For JSON types, attach a type guard

### Type-safe Client Operations
- `get`, `set`, `del`, `exists` for strings/JSON
- `hget`, `hset`, `hgetall`, `hdel`, `hexists` for hashes
- `lpush`, `rpush`, `lpop`, `rpop`, `lrange`, `llen` for lists
- `sadd`, `srem`, `smembers`, `sismember`, `scard` for sets
- `zadd`, `zrem`, `zrange`, `zrank`, `zscore` for sorted sets

### Utility
- `keyPattern(pattern: string)` — Define a branded key pattern for matching

## Advanced Usage

- **Custom Validators for JSON**
  ```typescript
  function isUser(obj: unknown): obj is { name: string; age: number } {
    return (
      typeof obj === "object" && obj !== null &&
      typeof (obj as any).name === "string" &&
      typeof (obj as any).age === "number"
    );
  }
  const userJson = redisJson<{ name: string; age: number }>().schema(isUser);
  ```

- **Hash Field Types**
  ```typescript
  const userHash = redisHash("user:hash", {
    name: redisHashString(),
    age: redisHashNumber(),
    profile: redisHashJson<{ bio: string }>(),
  });
  ```

## License

MIT
