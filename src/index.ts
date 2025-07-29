/**
 * A Redis key, which can be a string or a KeyPattern.
 */
export type RedisKey = string | KeyPattern;

/**
 * Represents a branded key pattern for Redis key matching.
 */
export interface KeyPattern {
  readonly _brand: "KeyPattern";
  readonly pattern: string;
}

/**
 * Base interface for all Redis data types.
 * @template T The TypeScript type represented by this Redis type.
 */
export interface RedisDataType<T = any> {
  readonly _type: T;
  readonly _redisType: string;
  readonly _optional?: boolean;
  readonly _default?: T;
  readonly _ttl?: number;
  readonly _description?: string;
  readonly _runtimeType?:
    | "string"
    | "number"
    | "object"
    | "boolean"
    | "unknown";
  readonly validator?: (value: unknown) => boolean;
}

/**
 * Operations for Redis JSON types.
 * @template T The TypeScript type.
 */
export interface RedisJsonOperations<T> {
  /**
   * Get the value of a JSON key.
   */
  get(key: string): Promise<T | null>;
  /**
   * Set the value of a JSON key.
   */
  set(key: string, value: T): Promise<void>;
  /**
   * Delete a JSON key.
   */
  del(key: string): Promise<number>;
  /**
   * Check if a JSON key exists.
   */
  exists(key: string): Promise<boolean>;
}

/**
 * Create a branded key pattern for Redis key matching.
 * @param pattern The key pattern string.
 */
export function keyPattern(pattern: string): KeyPattern {
  return { _brand: "KeyPattern", pattern };
}

/**
 * Abstract base class for chainable Redis data types.
 * @template T The TypeScript type.
 * @template TSelf The concrete subclass type.
 */
abstract class ChainableRedisType<T, TSelf> implements RedisDataType<T> {
  readonly _type!: T;
  abstract readonly _redisType: string;
  readonly _optional?: boolean;
  readonly _default?: T;
  readonly _ttl?: number;
  readonly _description?: string;

  constructor(
    public config: {
      optional?: boolean;
      default?: T;
      ttl?: number;
      description?: string;
      [key: string]: any;
    } = {}
  ) {
    this._optional = config.optional;
    this._default = config.default;
    this._ttl = config.ttl;
    this._description = config.description;
  }

  protected abstract _clone(newConfig: any): TSelf;

  /**
   * Mark this type as optional.
   */
  optional(): TSelf {
    return this._clone({ ...this.config, optional: true });
  }

  /**
   * Set a default value for this type.
   * @param value The default value.
   */
  default(value: T): TSelf {
    return this._clone({ ...this.config, default: value });
  }

  /**
   * Set a TTL (time-to-live) in seconds for this type.
   * @param seconds TTL in seconds.
   */
  ttl(seconds: number): TSelf {
    return this._clone({ ...this.config, ttl: seconds });
  }

  /**
   * Add a description for this type.
   * @param desc The description string.
   */
  description(desc: string): TSelf {
    return this._clone({ ...this.config, description: desc });
  }
}

/**
 * Represents a Redis string type.
 * @template T The TypeScript type (default: string).
 */
export class RedisStringType<T = string> extends ChainableRedisType<
  T,
  RedisStringType<T>
> {
  readonly _redisType = "string";
  readonly _runtimeType = "string";

  protected _clone(newConfig: any): RedisStringType<T> {
    return new RedisStringType<T>(newConfig);
  }

  /**
   * Set the minimum length constraint for the string.
   * @param length Minimum length.
   */
  minLength(length: number): RedisStringType<T> {
    return this._clone({ ...this.config, minLength: length });
  }

  /**
   * Set the maximum length constraint for the string.
   * @param length Maximum length.
   */
  maxLength(length: number): RedisStringType<T> {
    return this._clone({ ...this.config, maxLength: length });
  }
}

/**
 * Create a Redis string type.
 * @template T The TypeScript type (default: string).
 */
export function redisString<T = string>(): RedisStringType<T> {
  return new RedisStringType<T>();
}

/**
 * Represents a field in a Redis hash.
 * @template T The TypeScript type of the field.
 */
export class RedisHashField<T> extends ChainableRedisType<
  T,
  RedisHashField<T>
> {
  readonly _redisType = "hash-field";
  readonly _runtimeType: "string" | "number" | "object" | "boolean" | "unknown";
  constructor(config: any = {}) {
    super(config);
    this._runtimeType = config.runtimeType || "unknown";
  }
  protected _clone(newConfig: any): RedisHashField<T> {
    return new RedisHashField<T>(newConfig);
  }
}

/**
 * Create a Redis hash field of type string.
 */
export function redisHashString(): RedisHashField<string> {
  return new RedisHashField<string>({ runtimeType: "string" });
}

/**
 * Create a Redis hash field of type number.
 */
export function redisHashNumber(): RedisHashField<number> {
  return new RedisHashField<number>({ runtimeType: "number" });
}

/**
 * Create a Redis hash field of type object (JSON).
 * @template T The TypeScript type of the JSON object.
 */
export function redisHashJson<T>(): RedisHashField<T> {
  return new RedisHashField<T>({ runtimeType: "object" });
}

/**
 * Interface for a Redis hash type.
 * @template TFields The fields of the hash.
 */
export interface RedisHashType<
  TFields extends Record<string, RedisHashField<any>>
> extends RedisDataType<TFields> {
  readonly _redisType: "hash";
  readonly fields: TFields;
  readonly key: RedisKey;
}

/**
 * Builder for Redis hash types.
 * @template TFields The fields of the hash.
 */
export class RedisHashBuilder<
    TFields extends Record<string, RedisHashField<any>>
  >
  extends ChainableRedisType<TFields, RedisHashBuilder<TFields>>
  implements RedisHashType<TFields>
{
  readonly _redisType = "hash";

  constructor(
    readonly key: RedisKey,
    readonly fields: TFields,
    config: any = {}
  ) {
    super(config);
  }

  protected _clone(newConfig: any): RedisHashBuilder<TFields> {
    return new RedisHashBuilder(this.key, this.fields, newConfig);
  }

  /**
   * Mark fields as indexed for search or lookup.
   * @param fields The field names to index.
   */
  index(...fields: (keyof TFields)[]): RedisHashBuilder<TFields> {
    return this._clone({ ...this.config, indexedFields: fields });
  }
}

/**
 * Create a Redis hash type.
 * @template TFields The fields of the hash.
 * @param key The Redis key or pattern.
 * @param fields The hash fields.
 */
export function redisHash<TFields extends Record<string, RedisHashField<any>>>(
  key: RedisKey,
  fields: TFields
): RedisHashBuilder<TFields> {
  return new RedisHashBuilder(key, fields);
}

/**
 * Represents a Redis list type.
 * @template T The element type.
 */
export class RedisListType<T> extends ChainableRedisType<
  T[],
  RedisListType<T>
> {
  readonly _redisType = "list";

  constructor(
    readonly key: RedisKey,
    readonly elementType: RedisDataType<T>,
    config: any = {}
  ) {
    super(config);
  }

  protected _clone(newConfig: any): RedisListType<T> {
    return new RedisListType(this.key, this.elementType, newConfig);
  }

  /**
   * Set the maximum length constraint for the list.
   * @param length Maximum length.
   */
  maxLength(length: number): RedisListType<T> {
    return this._clone({ ...this.config, maxLength: length });
  }

  /**
   * Set the list mode to FIFO.
   */
  fifo(): RedisListType<T> {
    return this._clone({ ...this.config, mode: "fifo" });
  }

  /**
   * Set the list mode to LIFO.
   */
  lifo(): RedisListType<T> {
    return this._clone({ ...this.config, mode: "lifo" });
  }
}

/**
 * Create a Redis list type.
 * @template T The element type.
 * @param key The Redis key or pattern.
 * @param elementType The type of elements in the list.
 */
export function redisList<T>(
  key: RedisKey,
  elementType: RedisDataType<T>
): RedisListType<T> {
  return new RedisListType(key, elementType);
}

/**
 * Represents a Redis set type.
 * @template T The element type.
 */
export class RedisSetType<T> extends ChainableRedisType<T[], RedisSetType<T>> {
  readonly _redisType = "set";

  constructor(
    readonly key: RedisKey,
    readonly elementType: RedisDataType<T>,
    config: any = {}
  ) {
    super(config);
  }

  protected _clone(newConfig: any): RedisSetType<T> {
    return new RedisSetType(this.key, this.elementType, newConfig);
  }

  /**
   * Set the maximum size constraint for the set.
   * @param size Maximum size.
   */
  maxSize(size: number): RedisSetType<T> {
    return this._clone({ ...this.config, maxSize: size });
  }
}

/**
 * Create a Redis set type.
 * @template T The element type.
 * @param key The Redis key or pattern.
 * @param elementType The type of elements in the set.
 */
export function redisSet<T>(
  key: RedisKey,
  elementType: RedisDataType<T>
): RedisSetType<T> {
  return new RedisSetType(key, elementType);
}

/**
 * Represents a Redis sorted set (zset) type.
 * @template T The element type.
 */
export class RedisSortedSetType<T> extends ChainableRedisType<
  T[],
  RedisSortedSetType<T>
> {
  readonly _redisType = "zset";

  constructor(
    readonly key: RedisKey,
    readonly elementType: RedisDataType<T>,
    config: any = {}
  ) {
    super(config);
  }

  protected _clone(newConfig: any): RedisSortedSetType<T> {
    return new RedisSortedSetType(this.key, this.elementType, newConfig);
  }

  /**
   * Set the maximum size constraint for the sorted set.
   * @param size Maximum size.
   */
  maxSize(size: number): RedisSortedSetType<T> {
    return this._clone({ ...this.config, maxSize: size });
  }
}

/**
 * Create a Redis sorted set type.
 * @template T The element type.
 * @param key The Redis key or pattern.
 * @param elementType The type of elements in the sorted set.
 */
export function redisSortedSet<T>(
  key: RedisKey,
  elementType: RedisDataType<T>
): RedisSortedSetType<T> {
  return new RedisSortedSetType(key, elementType);
}

/**
 * Represents a Redis JSON type.
 * @template T The TypeScript type of the JSON object.
 */
export class RedisJsonType<T> extends ChainableRedisType<T, RedisJsonType<T>> {
  readonly _redisType = "json";
  readonly _runtimeType = "object";
  readonly validator?: (value: unknown) => boolean;

  constructor(config: any = {}) {
    super(config);
    this.validator = config.validator;
  }

  protected _clone(newConfig: any): RedisJsonType<T> {
    return new RedisJsonType<T>(newConfig);
  }

  /**
   * Attach a schema validator to this JSON type.
   * @template U The new type after validation.
   * @param validator A type guard function.
   */
  schema<U>(validator: (value: unknown) => value is U): RedisJsonType<U> {
    // Remove default if type is incompatible
    const { default: def, ...rest } = this.config;
    return new RedisJsonType<U>({ ...rest, validator });
  }
}

/**
 * Create a Redis JSON type.
 * @template T The TypeScript type of the JSON object.
 */
export function redisJson<T>(): RedisJsonType<T> {
  return new RedisJsonType<T>();
}

/**
 * A type-safe Redis client for a given schema.
 * @template TSchema The schema definition.
 */
export interface RedisClient<TSchema extends Record<string, RedisDataType>> {
  readonly schema: {
    [K in keyof TSchema]: TSchema[K] extends RedisStringType<infer T>
      ? RedisStringOperations<T>
      : TSchema[K] extends RedisHashBuilder<infer TFields>
      ? RedisHashOperations<TFields>
      : TSchema[K] extends RedisListType<infer T>
      ? RedisListOperations<T>
      : TSchema[K] extends RedisSetType<infer T>
      ? RedisSetOperations<T>
      : TSchema[K] extends RedisSortedSetType<infer T>
      ? RedisSortedSetOperations<T>
      : TSchema[K] extends RedisJsonType<infer T>
      ? RedisJsonOperations<T>
      : never;
  };
}

/**
 * Operations for Redis string types.
 * @template T The TypeScript type.
 */
export interface RedisStringOperations<T> {
  /**
   * Get the value of a string key.
   */
  get(key: string): Promise<T | null>;
  /**
   * Set the value of a string key.
   */
  set(key: string, value: T): Promise<void>;
  /**
   * Delete a string key.
   */
  del(key: string): Promise<number>;
  /**
   * Check if a string key exists.
   */
  exists(key: string): Promise<boolean>;
}

/**
 * Operations for Redis hash types.
 * @template TFields The hash fields.
 */
export interface RedisHashOperations<
  TFields extends Record<string, RedisHashField<any>>
> {
  /**
   * Get the value of a hash field.
   */
  hget<K extends keyof TFields>(
    key: string,
    field: K
  ): Promise<TFields[K]["_type"] | null>;
  /**
   * Set the value of a hash field.
   */
  hset<K extends keyof TFields>(
    key: string,
    field: K,
    value: TFields[K]["_type"]
  ): Promise<void>;
  /**
   * Set multiple hash fields.
   */
  hset(
    key: string,
    fields: { [K in keyof TFields]?: TFields[K]["_type"] }
  ): Promise<void>;
  /**
   * Get all fields and values of a hash.
   */
  hgetall(
    key: string
  ): Promise<{ [K in keyof TFields]: TFields[K]["_type"] } | null>;
  /**
   * Delete one or more hash fields.
   */
  hdel(key: string, ...fields: (keyof TFields)[]): Promise<number>;
  /**
   * Check if a hash field exists.
   */
  hexists(key: string, field: keyof TFields): Promise<boolean>;
}

/**
 * Operations for Redis list types.
 * @template T The element type.
 */
export interface RedisListOperations<T = any> {
  lpush(key: string, ...values: any[]): Promise<number>;
  rpush(key: string, ...values: any[]): Promise<number>;
  lpop(key: string): Promise<any | null>;
  rpop(key: string): Promise<any | null>;
  lrange(key: string, start: number, stop: number): Promise<any[]>;
  llen(key: string): Promise<number>;
}

/**
 * Operations for Redis set types.
 * @template T The element type.
 */
export interface RedisSetOperations<T = any> {
  sadd(key: string, ...members: any[]): Promise<number>;
  srem(key: string, ...members: any[]): Promise<number>;
  smembers(key: string): Promise<any[]>;
  sismember(key: string, member: any): Promise<boolean>;
  scard(key: string): Promise<number>;
}

/**
 * Operations for Redis sorted set (zset) types.
 * @template T The element type.
 */
export interface RedisSortedSetOperations<T = any> {
  zadd(key: string, score: number, member: any): Promise<number>;
  zrem(key: string, ...members: any[]): Promise<number>;
  zrange(key: string, start: number, stop: number): Promise<any[]>;
  zrank(key: string, member: any): Promise<number | null>;
  zscore(key: string, member: any): Promise<number | null>;
}

import { createClient, type RedisClientType } from "@redis/client";

/**
 * Create a type-safe Redis client for a given schema.
 * @template TSchema The schema definition.
 * @param connectionString The Redis connection string.
 * @param schema The schema definition.
 */
export function createRedisClient<
  TSchema extends Record<string, RedisDataType>
>(
  connectionString: string,
  schema: TSchema
): RedisClient<TSchema> & {
  /**
   * Disconnect the Redis client.
   */
  quit(): Promise<void>;
  /**
   * Ping the Redis server.
   */
  ping(): Promise<string>;
  /**
   * Check if the client is connected.
   */
  isConnected(): boolean;
  /**
   * Reconnect the client.
   */
  reconnect(): Promise<void>;
} {
  const client = createClient({ url: connectionString });
  let connected = false;

  // Ensure connection on first operation
  const ensureConnection = async () => {
    if (!connected) {
      try {
        await client.connect();
        connected = true;
      } catch (err) {
        console.error("Redis connection error:", err);
        throw err;
      }
    }
  };

  // Expose connection state
  const isConnected = () => connected;

  // Reconnect logic
  const reconnect = async () => {
    if (connected) {
      client.quit();
      connected = false;
    }
    await ensureConnection();
  };

  // Serialization helpers
  const serialize = (value: any, dataType: RedisDataType): string => {
    if (
      dataType._redisType === "json" ||
      (dataType._redisType === "hash-field" &&
        dataType._runtimeType === "object")
    ) {
      try {
        return JSON.stringify(value);
      } catch (err) {
        console.error("Serialization error:", err);
        throw err;
      }
    }
    return String(value);
  };

  const deserialize = <T>(
    value: string | null,
    dataType: RedisDataType<T>
  ): T | null => {
    if (value === null) return null;

    try {
      if (
        dataType._redisType === "json" ||
        (dataType._redisType === "hash-field" &&
          dataType._runtimeType === "object")
      ) {
        const parsed = JSON.parse(value);
        if (typeof dataType.validator === "function") {
          if (!dataType.validator(parsed)) {
            console.warn("Validation failed for JSON value:", parsed);
            return null;
          }
        }
        return parsed;
      }

      // Handle number conversion for hash fields
      if (
        dataType._redisType === "hash-field" &&
        dataType._runtimeType === "number"
      ) {
        const num = Number(value);
        return isNaN(num) ? (value as T) : (num as T);
      }

      // Handle boolean conversion for hash fields
      if (
        dataType._redisType === "hash-field" &&
        dataType._runtimeType === "boolean"
      ) {
        return (value === "true") as T;
      }

      // Default: string
      return value as T;
    } catch (err) {
      console.error(
        "Deserialization error:",
        err,
        "Value:",
        value,
        "Type:",
        dataType
      );
      return null;
    }
  };

  // Handle TTL setting
  const setWithTTL = async (key: string, value: string, ttl?: number) => {
    try {
      if (ttl) {
        await client.setEx(key, ttl, value);
      } else {
        await client.set(key, value);
      }
    } catch (err) {
      console.error("Redis set error:", err);
      throw err;
    }
  };

  // Build schema operations
  const schemaOperations: any = {};

  for (const [schemaKey, definition] of Object.entries(schema)) {
    const def = definition as RedisDataType;

    if (def._redisType === "json") {
      const jsonDef = def as RedisJsonType<any>;
      schemaOperations[schemaKey] = {
        /**
         * Get the value of a JSON key.
         */
        async get(key: string) {
          await ensureConnection();
          try {
            const value = await client.get(key);
            if (value === null && jsonDef._default !== undefined) {
              return jsonDef._default;
            }
            return deserialize(value, jsonDef);
          } catch (err) {
            console.error("Redis JSON GET error:", err);
            throw err;
          }
        },

        /**
         * Set the value of a JSON key.
         */
        async set(key: string, value: any) {
          await ensureConnection();
          try {
            const serialized = serialize(value, jsonDef);
            await setWithTTL(key, serialized, jsonDef._ttl);
          } catch (err) {
            console.error("Redis JSON SET error:", err);
            throw err;
          }
        },

        /**
         * Delete a JSON key.
         */
        async del(key: string) {
          await ensureConnection();
          try {
            return await client.del(key);
          } catch (err) {
            console.error("Redis JSON DEL error:", err);
            throw err;
          }
        },

        /**
         * Check if a JSON key exists.
         */
        async exists(key: string) {
          await ensureConnection();
          try {
            return (await client.exists(key)) === 1;
          } catch (err) {
            console.error("Redis JSON EXISTS error:", err);
            throw err;
          }
        },
      } satisfies RedisJsonOperations<any>;
    }
    if (def._redisType === "string") {
      const stringDef = def as RedisStringType;
      schemaOperations[schemaKey] = {
        /**
         * Get the value of a string key.
         */
        async get(key: string) {
          await ensureConnection();
          try {
            const value = await client.get(key);
            if (value === null && stringDef._default !== undefined) {
              return stringDef._default;
            }
            return deserialize(value, stringDef);
          } catch (err) {
            console.error("Redis GET error:", err);
            throw err;
          }
        },

        async set(key: string, value: any) {
          await ensureConnection();
          try {
            const serialized = serialize(value, stringDef);
            await setWithTTL(key, serialized, stringDef._ttl);
          } catch (err) {
            console.error("Redis SET error:", err);
            throw err;
          }
        },

        async del(key: string) {
          await ensureConnection();
          try {
            return await client.del(key);
          } catch (err) {
            console.error("Redis DEL error:", err);
            throw err;
          }
        },

        async exists(key: string) {
          await ensureConnection();
          try {
            return (await client.exists(key)) === 1;
          } catch (err) {
            console.error("Redis EXISTS error:", err);
            throw err;
          }
        },
      } satisfies RedisStringOperations<any>;
    }

    if (def._redisType === "hash") {
      const hashDef = def as RedisHashBuilder<any>;
      schemaOperations[schemaKey] = {
        async hget<K extends keyof typeof hashDef.fields>(
          key: string,
          field: K
        ) {
          await ensureConnection();
          try {
            const value = await client.hGet(key, field as string);
            const fieldDef = hashDef.fields[field] as RedisHashField<any>;
            if (value === null && fieldDef?._default !== undefined) {
              return fieldDef._default;
            }
            return deserialize(value, fieldDef);
          } catch (err) {
            console.error("Redis HGET error:", err);
            throw err;
          }
        },

        async hset(
          key: string,
          fieldOrFields: string | Record<string, any>,
          value?: any
        ) {
          await ensureConnection();
          try {
            if (typeof fieldOrFields === "string") {
              // Single field set
              const fieldDef = hashDef.fields[fieldOrFields];
              const serialized = serialize(value, fieldDef);
              await client.hSet(key, fieldOrFields, serialized);
            } else {
              // Multiple fields set
              const serializedFields: Record<string, string> = {};
              for (const [field, val] of Object.entries(fieldOrFields)) {
                const fieldDef = hashDef.fields[field];
                serializedFields[field] = serialize(val, fieldDef);
              }
              await client.hSet(key, serializedFields);
            }

            // Set TTL on the hash key if specified
            if (hashDef._ttl) {
              await client.expire(key, hashDef._ttl);
            }
          } catch (err) {
            console.error("Redis HSET error:", err);
            throw err;
          }
        },

        async hgetall(key: string) {
          await ensureConnection();
          try {
            const hash = await client.hGetAll(key);
            if (Object.keys(hash).length === 0) return null;

            const result: any = {};
            for (const [field, value] of Object.entries(hash)) {
              const fieldDef = hashDef.fields[field];
              result[field] = deserialize(value, fieldDef);
            }

            // Fill in defaults for missing optional fields
            for (const [field, fieldDef] of Object.entries(hashDef.fields)) {
              const typedFieldDef = fieldDef as RedisHashField<any>;
              if (!(field in result) && typedFieldDef._default !== undefined) {
                result[field] = typedFieldDef._default;
              }
            }

            return result;
          } catch (err) {
            console.error("Redis HGETALL error:", err);
            throw err;
          }
        },

        async hdel(key: string, ...fields: string[]) {
          await ensureConnection();
          try {
            return await client.hDel(key, fields);
          } catch (err) {
            console.error("Redis HDEL error:", err);
            throw err;
          }
        },

        async hexists<K extends keyof typeof hashDef.fields>(
          key: string,
          field: K
        ): Promise<boolean> {
          await ensureConnection();
          try {
            return (await client.hExists(key, field as string)) === 1;
          } catch (err) {
            console.error("Redis HEXISTS error:", err);
            throw err;
          }
        },
      } satisfies RedisHashOperations<any>;
    }

    if (def._redisType === "list") {
      const listDef = def as RedisListType<any>;
      schemaOperations[schemaKey] = {
        async lpush(key: string, ...values: any[]) {
          await ensureConnection();
          const serialized = values.map((v) =>
            serialize(v, listDef.elementType)
          );
          if (serialized.length === 0) return 0;
          const length = await client.lPush.apply(client, [
            key,
            ...serialized,
          ] as any);

          // Handle max length constraint (trim to last N elements)
          if (listDef.config?.maxLength) {
            await client.lTrim(key, -listDef.config.maxLength, -1);
          }

          // Set TTL if specified
          if (listDef._ttl) {
            await client.expire(key, listDef._ttl);
          }

          return length;
        },

        async rpush(key: string, ...values: any[]) {
          await ensureConnection();
          const serialized = values.map((v) =>
            serialize(v, listDef.elementType)
          );
          if (serialized.length === 0) return 0;
          const length = await client.rPush.apply(client, [
            key,
            ...serialized,
          ] as any);

          // Handle max length constraint
          if (listDef.config?.maxLength) {
            await client.lTrim(key, -listDef.config.maxLength, -1);
          }

          if (listDef._ttl) {
            await client.expire(key, listDef._ttl);
          }

          return length;
        },

        async lpop(key: string) {
          await ensureConnection();
          const value = await client.lPop(key);
          return deserialize(value, listDef.elementType);
        },

        async rpop(key: string) {
          await ensureConnection();
          const value = await client.rPop(key);
          return deserialize(value, listDef.elementType);
        },

        async lrange(key: string, start: number, stop: number) {
          await ensureConnection();
          const values = await client.lRange(key, start, stop);
          return values.map((v) => deserialize(v, listDef.elementType));
        },

        async llen(key: string) {
          await ensureConnection();
          return await client.lLen(key);
        },
      } satisfies RedisListOperations<any>;
    }

    if (def._redisType === "set") {
      const setDef = def as RedisSetType<any>;
      schemaOperations[schemaKey] = {
        async sadd(key: string, ...members: any[]) {
          await ensureConnection();
          const serialized = members.map((m) =>
            serialize(m, setDef.elementType)
          );
          if (serialized.length === 0) return 0;
          const count = await client.sAdd.apply(client, [
            key,
            ...serialized,
          ] as any);

          // Handle max size constraint (batch removal)
          if (setDef.config?.maxSize) {
            const currentSize = await client.sCard(key);
            if (currentSize > setDef.config.maxSize) {
              const excess = currentSize - setDef.config.maxSize;
              // Batch sPop if supported (Redis >= 3.2)
              if (excess > 0) {
                for (let i = 0; i < excess; i++) {
                  await client.sPop(key);
                }
              }
            }
          }

          if (setDef._ttl) {
            await client.expire(key, setDef._ttl);
          }

          return count;
        },

        async srem(key: string, ...members: any[]) {
          await ensureConnection();
          const serialized = members.map((m) =>
            serialize(m, setDef.elementType)
          );
          return await client.sRem(key, serialized);
        },

        async smembers(key: string) {
          await ensureConnection();
          const members = await client.sMembers(key);
          return members.map((m) => deserialize(m, setDef.elementType));
        },

        async sismember(key: string, member: any): Promise<boolean> {
          await ensureConnection();
          const serialized = serialize(member, setDef.elementType);
          return (await client.sIsMember(key, serialized)) === 1;
        },

        async scard(key: string) {
          await ensureConnection();
          return await client.sCard(key);
        },
      } satisfies RedisSetOperations<any>;
    }

    if (def._redisType === "zset") {
      const zsetDef = def as RedisSortedSetType<any>;
      schemaOperations[schemaKey] = {
        async zadd(key: string, score: number, member: any) {
          await ensureConnection();
          const serialized = serialize(member, zsetDef.elementType);
          const count = await client.zAdd(key, [{ score, value: serialized }]);

          // Handle max size constraint (batch removal)
          if (zsetDef.config?.maxSize) {
            const currentSize = await client.zCard(key);
            if (currentSize > zsetDef.config.maxSize) {
              const excess = currentSize - zsetDef.config.maxSize;
              if (excess > 0) {
                // Remove all excess in one call
                await client.zRemRangeByRank(key, 0, excess - 1);
              }
            }
          }

          if (zsetDef._ttl) {
            await client.expire(key, zsetDef._ttl);
          }

          return count;
        },

        async zrem(key: string, ...members: any[]) {
          await ensureConnection();
          const serialized = members.map((m) =>
            serialize(m, zsetDef.elementType)
          );
          return await client.zRem(key, serialized);
        },

        async zrange(key: string, start: number, stop: number) {
          await ensureConnection();
          const members = await client.zRange(key, start, stop);
          return members.map((m) => deserialize(m, zsetDef.elementType));
        },

        async zrank(key: string, member: any) {
          await ensureConnection();
          const serialized = serialize(member, zsetDef.elementType);
          return await client.zRank(key, serialized);
        },

        async zscore(key: string, member: any) {
          await ensureConnection();
          const serialized = serialize(member, zsetDef.elementType);
          return await client.zScore(key, serialized);
        },
      } satisfies RedisSortedSetOperations<any>;
    }
  }

  return {
    schema: schemaOperations,
    /**
     * Quit the Redis client.
     */
    async quit() {
      if (connected) {
        try {
          await client.quit();
          connected = false;
        } catch (err) {
          console.error("Redis disconnect error:", err);
          throw err;
        }
      }
    },
    /**
     * Ping the Redis server.
     */
    async ping() {
      await ensureConnection();
      try {
        return await client.ping();
      } catch (err) {
        console.error("Redis ping error:", err);
        throw err;
      }
    },
    /**
     * Check if the client is connected.
     */
    isConnected,
    /**
     * Reconnect the client.
     */
    reconnect,
  } as RedisClient<TSchema> & {
    quit(): Promise<void>;
    ping(): Promise<string>;
    isConnected(): boolean;
    reconnect(): Promise<void>;
  };
}
