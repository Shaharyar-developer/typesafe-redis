import { describe, it, expect, mock, beforeEach } from "bun:test";
import {
  createRedisClient,
  redisString,
  redisHash,
  redisHashString,
  redisHashNumber,
  redisList,
  redisSet,
  redisSortedSet,
  redisJson,
} from "../src/index";

// Mock @redis/client
const mockConnect = mock(async () => {});
const mockQuit = mock(async () => {});
const mockSet = mock(async () => {});
const mockGet = mock(async () => null);
const mockDel = mock(async () => 1);
const mockExists = mock(async () => 1);
const mockHSet = mock(async () => {});
const mockLTrim = mock(async () => {});
const mockExpire = mock(async () => {});
const mockSAdd = mock(async () => 1);
const mockSRem = mock(async () => 1);
const mockSMembers = mock(async () => []);
const mockZAdd = mock(async () => 1);
const mockZRem = mock(async () => 1);
const mockZRange = mock(async () => []);
const mockZRank = mock(async () => 0);
const mockZScore = mock(async () => 1);

const mockSCard = mock(async () => 2);
const mockSPop = mock(async () => undefined);
const mockZCard = mock(async () => 2);
const mockZRemRangeByRank = mock(async () => undefined);
const mockRedisClient = {
  connect: mockConnect,
  quit: mockQuit,
  set: mockSet,
  get: mockGet,
  del: mockDel,
  exists: mockExists,
  hSet: mockHSet,
  lTrim: mockLTrim,
  expire: mockExpire,
  sAdd: mockSAdd,
  sRem: mockSRem,
  sMembers: mockSMembers,
  sCard: mockSCard,
  zAdd: mockZAdd,
  zRem: mockZRem,
  zRange: mockZRange,
  zRank: mockZRank,
  zScore: mockZScore,
  sPop: mockSPop,
  zCard: mockZCard,
  zRemRangeByRank: mockZRemRangeByRank,
} as any;

const mockCreateClient = mock(() => mockRedisClient);
mock.module("@redis/client", () => ({
  createClient: mockCreateClient,
}));

describe("uncovered branches", () => {
  beforeEach(() => {
    mockConnect.mockClear();
    mockQuit.mockClear();
    mockSet.mockClear();
    mockGet.mockClear();
    mockDel.mockClear();
    mockExists.mockClear();
    mockHSet.mockClear();
    mockLTrim.mockClear();
    mockExpire.mockClear();
    mockSAdd.mockClear();
    mockSRem.mockClear();
    mockSMembers.mockClear();
    mockSCard.mockClear();
    mockZAdd.mockClear();
    mockZRem.mockClear();
    mockZRange.mockClear();
    mockZRank.mockClear();
    mockZScore.mockClear();
    mockSPop.mockClear();
    mockZCard.mockClear();
    mockZRemRangeByRank.mockClear();
  });

  it("should call .optional(), .ttl(), .description(), .minLength(), .maxLength()", () => {
    const t = redisString<string>()
      .optional()
      .ttl(5)
      .description("desc")
      .minLength(1)
      .maxLength(10);
    expect(t.config.optional).toBe(true);
    expect(t.config.ttl).toBe(5);
    expect(t.config.description).toBe("desc");
    expect(t.config.minLength).toBe(1);
    expect(t.config.maxLength).toBe(10);
  });

  it("should call .fifo() and .lifo() on list", () => {
    const l = redisList("k", redisString()).fifo().lifo();
    expect(l.config.mode).toBe("lifo");
  });

  it("should call .index() on hash and .maxSize() on set/zset", () => {
    const h = redisHash("k", { a: redisHashString() }).index("a");
    expect(h.config.indexedFields).toEqual(["a"]);
    const s = redisSet("k", redisString()).maxSize(2);
    expect(s.config.maxSize).toBe(2);
    const z = redisSortedSet("k", redisString()).maxSize(3);
    expect(z.config.maxSize).toBe(3);
  });

  it("should handle TTL and maxLength for list", async () => {
    const client = createRedisClient("redis://localhost", {
      l: redisList("l", redisString()).ttl(10).maxLength(2),
    });
    mockRedisClient.lPush = mock(async () => 3);
    mockRedisClient.lTrim = mockLTrim;
    mockRedisClient.expire = mockExpire;
    await client.schema.l.lpush("l", "a", "b", "c");
    // lTrim should be called if maxLength is set and lPush returns > maxLength
    try {
      expect(mockLTrim).toHaveBeenCalledWith("l", -2, -1);
    } catch (e) {
      // Print actual calls for debugging
      // eslint-disable-next-line no-console
      console.error("lTrim calls:", mockLTrim.mock.calls);
      throw e;
    }
    expect(mockExpire).toHaveBeenCalledWith("l", 10);
    // If this fails, check that your implementation calls lTrim when lPush > maxLength
  });

  it("should handle maxSize for set and zset", async () => {
    const client = createRedisClient("redis://localhost", {
      s: redisSet("s", redisString()).maxSize(1),
      z: redisSortedSet("z", redisString()).maxSize(1),
    });
    mockRedisClient.sAdd = mock(async () => 2);
    mockRedisClient.sMembers = mock(async () => ["a", "b"]);
    mockRedisClient.sRem = mockSRem;
    mockRedisClient.sCard = mockSCard;
    mockRedisClient.sPop = mockSPop;
    await client.schema.s.sadd("s", "a", "b");
    expect(mockSPop).toHaveBeenCalled(); // sPop is used for excess removal
    mockRedisClient.zAdd = mock(async () => 2);
    mockRedisClient.zRange = mock(async () => ["a", "b"]);
    mockRedisClient.zRem = mockZRem;
    mockRedisClient.zRemRangeByRank = mockZRemRangeByRank;
    await client.schema.z.zadd("z", 1, "a");
    expect(mockZRemRangeByRank).toHaveBeenCalled(); // zRemRangeByRank is used for excess removal
  });

  it("should handle reconnect logic", async () => {
    const client = createRedisClient("redis://localhost", {
      foo: redisString(),
    });
    await client.schema.foo.get("foo-key");
    await client.reconnect();
    expect(mockQuit).toHaveBeenCalled();
    expect(mockConnect).toHaveBeenCalled();
  });

  it("should handle error in set", async () => {
    const client = createRedisClient("redis://localhost", {
      foo: redisString(),
    });
    mockSet.mockImplementationOnce(async () => {
      throw new Error("fail");
    });
    await expect(client.schema.foo.set("foo-key", "bar")).rejects.toThrow(
      "fail"
    );
  });

  it("should handle error in del", async () => {
    const client = createRedisClient("redis://localhost", {
      foo: redisString(),
    });
    mockDel.mockImplementationOnce(async () => {
      throw new Error("fail");
    });
    await expect(client.schema.foo.del("foo-key")).rejects.toThrow("fail");
  });

  it("should handle empty input for lpush/sadd/zadd", async () => {
    const client = createRedisClient("redis://localhost", {
      l: redisList("l", redisString()),
      s: redisSet("s", redisString()),
      z: redisSortedSet("z", redisString()),
    });
    mockRedisClient.lPush = mock(async () => 0);
    mockRedisClient.sAdd = mock(async () => 0);
    mockRedisClient.zAdd = mock(async () => 0);
    expect(await client.schema.l.lpush("l")).toBe(0);
    expect(await client.schema.s.sadd("s")).toBe(0);
    expect(await client.schema.z.zadd("z", 1, undefined)).toBe(0);
  });
});
