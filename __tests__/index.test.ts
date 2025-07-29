import { describe, it, expect, mock, beforeEach, afterEach } from "bun:test";
import {
  createRedisClient,
  redisString,
  redisHash,
  redisHashString,
  redisHashNumber,
  redisSet,
  redisSortedSet,
  redisJson,
  redisList,
} from "../src/index";

// Mock @redis/client
const mockConnect = mock(async () => {});
const mockDisconnect = mock(async () => {});
const mockQuit = mock(async () => {
  console.log("Mocked quit called");
});
const mockPing = mock(async () => "PONG");
const mockSet = mock(async () => {});
const mockGet = mock<(...args: any[]) => Promise<string | null>>(
  async () => null
);
const mockDel = mock(async () => 1);
const mockExists = mock(async () => 1);
const mockHGet = mock<(...args: any[]) => Promise<string | null>>(
  async () => null
);
const mockHSet = mock(async () => {});
const mockHGetAll = mock(async () => ({}));
const mockHDel = mock(async () => 1);
const mockHExists = mock(async () => 1);
const mockExpire = mock(async () => {});
const mockLpush = mock(async () => 1);
const mockLrange = mock(async () => []);
const mockSadd = mock(async () => 1);
const mockSmembers = mock(async () => []);
const mockZadd = mock(async () => 1);
const mockZrange = mock(async () => []);

const mockRedisClient = {
  connect: mockConnect,
  quit: mockQuit,
  ping: mockPing,
  set: mockSet,
  get: mockGet,
  del: mockDel,
  exists: mockExists,
  hGet: mockHGet,
  hSet: mockHSet,
  hGetAll: mockHGetAll,
  hDel: mockHDel,
  hExists: mockHExists,
  expire: mockExpire,
  lPush: mockLpush,
  lRange: mockLrange,
  sAdd: mockSadd,
  sMembers: mockSmembers,
  zAdd: mockZadd,
  zRange: mockZrange,
} as any;

const mockCreateClient = mock(() => mockRedisClient);
mock.module("@redis/client", () => ({
  createClient: mockCreateClient,
}));

describe("createRedisClient", () => {
  beforeEach(() => {
    mockConnect.mockClear();
    mockDisconnect.mockClear();
    mockSet.mockClear();
    mockGet.mockClear();
    mockDel.mockClear();
    mockExists.mockClear();
    mockHGet.mockClear();
    mockHSet.mockClear();
    mockHGetAll.mockClear();
    mockHDel.mockClear();
    mockHExists.mockClear();
    mockExpire.mockClear();
  });

  it("should connect and set/get string values", async () => {
    mockGet.mockImplementationOnce(async () => "hello");
    const client = createRedisClient("redis://localhost", {
      foo: redisString<string>(),
    });

    await client.schema.foo.set("foo-key", "hello");
    expect(mockSet).toHaveBeenCalledWith("foo-key", "hello");

    const value = await client.schema.foo.get("foo-key");
    expect(value).toBe("hello");
    expect(mockGet).toHaveBeenCalledWith("foo-key");
  });

  it("should return default value for string if not set", async () => {
    mockGet.mockImplementationOnce(async () => null);
    const client = createRedisClient("redis://localhost", {
      bar: redisString<string>().default("default-value"),
    });

    const value = await client.schema.bar.get("bar-key");
    expect(value).toBe("default-value");
  });

  it("should call del and exists for string", async () => {
    const client = createRedisClient("redis://localhost", {
      foo: redisString<string>(),
    });

    const delResult = await client.schema.foo.del("foo-key");
    expect(delResult).toBe(1);
    expect(mockDel).toHaveBeenCalledWith("foo-key");

    const existsResult = await client.schema.foo.exists("foo-key");
    expect(existsResult).toBe(true);
    expect(mockExists).toHaveBeenCalledWith("foo-key");
  });

  it("should handle hash hget/hset/hgetall", async () => {
    mockHGet.mockImplementationOnce(async () => "bar");
    mockHGetAll.mockImplementationOnce(async () => ({ a: "1", b: "2" }));
    const client = createRedisClient("redis://localhost", {
      myhash: redisHash("myhash", {
        a: redisHashString(),
        b: redisHashNumber(),
      }),
    });

    const hgetValue = await client.schema.myhash.hget("myhash", "a");
    expect(hgetValue).toBe("bar");
    expect(mockHGet).toHaveBeenCalledWith("myhash", "a");

    await client.schema.myhash.hset("myhash", "a", "baz");
    expect(mockHSet).toHaveBeenCalledWith("myhash", "a", "baz");

    await client.schema.myhash.hset("myhash", { a: "baz", b: 42 });
    expect(mockHSet).toHaveBeenCalledWith("myhash", { a: "baz", b: "42" });

    const hgetallValue = await client.schema.myhash.hgetall("myhash");
    expect(hgetallValue).toEqual({ a: "1", b: 2 });
    expect(mockHGetAll).toHaveBeenCalledWith("myhash");
  });

  it("should call quit and ping", async () => {
    const client = createRedisClient("redis://localhost", {
      foo: redisString(),
    });

    // Ensure connection is established before quit
    await client.schema.foo.get("foo-key");

    await client.quit();
    expect(mockQuit).toHaveBeenCalled();

    const pong = await client.ping();
    expect(pong).toBe("PONG");
    expect(mockPing).toHaveBeenCalled();
  });
  it("should set and get list values", async () => {
    const client = createRedisClient("redis://localhost", {
      mylist: redisList("mylist", redisString<string>()),
    });
    const mockLpush = mock(async () => 1);
    const mockLrange = mock(async () => ["a", "b"]);
    mockRedisClient.lPush = mockLpush;
    mockRedisClient.lRange = mockLrange;

    await client.schema.mylist.lpush("mylist", "a", "b");
    expect(mockLpush).toHaveBeenCalledWith("mylist", "a", "b");

    const values = await client.schema.mylist.lrange("mylist", 0, -1);
    expect(values).toEqual(["a", "b"]);
    expect(mockLrange).toHaveBeenCalledWith("mylist", 0, -1);
  });

  it("should set and get set values", async () => {
    const client = createRedisClient("redis://localhost", {
      myset: redisSet("myset", redisString<string>()),
    });
    const mockSadd = mock(async () => 2);
    const mockSmembers = mock(async () => ["x", "y"]);
    mockRedisClient.sAdd = mockSadd;
    mockRedisClient.sMembers = mockSmembers;

    await client.schema.myset.sadd("myset", "x", "y");
    expect(mockSadd).toHaveBeenCalledWith("myset", "x", "y");

    const members = await client.schema.myset.smembers("myset");
    expect(members).toEqual(["x", "y"]);
    expect(mockSmembers).toHaveBeenCalledWith("myset");
  });

  it("should set and get sorted set values", async () => {
    const client = createRedisClient("redis://localhost", {
      myzset: redisSortedSet("myzset", redisString<string>()),
    });
    const mockZadd = mock(async () => 1);
    const mockZrange = mock(async () => ["foo"]);
    mockRedisClient.zAdd = mockZadd;
    mockRedisClient.zRange = mockZrange;

    await client.schema.myzset.zadd("myzset", 1, "foo");
    expect(mockZadd).toHaveBeenCalledWith("myzset", [
      { score: 1, value: "foo" },
    ]);

    const range = await client.schema.myzset.zrange("myzset", 0, -1);
    expect(range).toEqual(["foo"]);
    expect(mockZrange).toHaveBeenCalledWith("myzset", 0, -1);
  });

  it("should set and get JSON values with validator", async () => {
    const validator = (v: unknown): v is { foo: string } =>
      typeof v === "object" && v !== null && "foo" in v;
    const client = createRedisClient("redis://localhost", {
      myjson: redisJson<{ foo: string }>().schema(validator),
    });
    const mockSet = mock(async () => {});
    const mockGet = mock(async () => JSON.stringify({ foo: "bar" }));
    mockRedisClient.set = mockSet;
    mockRedisClient.get = mockGet;

    await client.schema.myjson.set("myjson", { foo: "bar" });
    expect(mockSet).toHaveBeenCalled();

    const value = await client.schema.myjson.get("myjson");
    expect(value).toEqual({ foo: "bar" });
  });

  it("should handle TTL when setting string", async () => {
    const client = createRedisClient("redis://localhost", {
      foo: redisString<string>().ttl(10),
    });
    const mockSetEx = mock(async () => {});
    mockRedisClient.setEx = mockSetEx;

    await client.schema.foo.set("foo-key", "bar");
    expect(mockSetEx).toHaveBeenCalledWith("foo-key", 10, "bar");
  });

  it("should handle reconnect and isConnected", async () => {
    const client = createRedisClient("redis://localhost", {
      foo: redisString<string>(),
    });
    await client.schema.foo.get("foo-key");
    expect(!!client.isConnected()).toBe(true);

    await client.reconnect();
    expect(mockQuit).toHaveBeenCalled();
    expect(client.isConnected()).toBe(true);
  });

  it("should handle deserialization error gracefully", async () => {
    const client = createRedisClient("redis://localhost", {
      foo: redisJson<{ foo: string }>(),
    });
    const mockGet = mock(async () => "{invalid json");
    mockRedisClient.get = mockGet;

    const value = await client.schema.foo.get("foo-key");
    expect(value).toBeNull();
  });
});
