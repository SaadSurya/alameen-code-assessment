package com.alameen.question3;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.params.SetParams;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class RateLimiter {
    private final JedisSentinelPool jedisPool;
    private static final String RATE_LIMITER_KEY_PREFIX = "rate_limiter:";
    private static final String LUA_SCRIPT =
            "local current\n" +
                    "current = redis.call('INCR', KEYS[1])\n" +
                    "if tonumber(current) == 1 then\n" +
                    "  redis.call('EXPIRE', KEYS[1], ARGV[1])\n" +
                    "end\n" +
                    "if tonumber(current) > tonumber(ARGV[2]) then\n" +
                    "  return 0\n" +
                    "else\n" +
                    "  return 1\n" +
                    "end";

    public RateLimiter(String sentinelHost, int sentinelPort, String masterName, String redisPassword) {
        var poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(60000);
        poolConfig.setTimeBetweenEvictionRunsMillis(30000);
        poolConfig.setNumTestsPerEvictionRun(3);

        this.jedisPool = new JedisSentinelPool(
                masterName,
                Collections.singleton("sentinel://" + sentinelHost + ":" + sentinelPort),
                poolConfig,
                2000,
                redisPassword
        );
    }

    public void setLimit(String tenantId, int requestsPerMinute) {
        try (var jedis = jedisPool.getResource()) {
            var key = RATE_LIMITER_KEY_PREFIX + tenantId + ":limit";
            jedis.set(key, String.valueOf(requestsPerMinute));
        }
    }

    public boolean allowRequest(String tenantId) {
        var now = Instant.now().getEpochSecond();
        var currentWindow = now / 60;
        var requestCountKey = RATE_LIMITER_KEY_PREFIX + tenantId + ":count:" + currentWindow;
        var rateLimitKey = RATE_LIMITER_KEY_PREFIX + tenantId + ":limit";

        try (var jedis = jedisPool.getResource()) {
            var rateLimit = Integer.parseInt(jedis.get(rateLimitKey));
            var result = jedis.eval(LUA_SCRIPT, Collections.singletonList(requestCountKey),
                    Arrays.asList("60", String.valueOf(rateLimit)));
            return (Long) result == 1;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
