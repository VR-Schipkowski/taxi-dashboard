package com.taxifleet.helper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.io.Serializable;

public class RedisSink implements Serializable {
    private static final long serialVersionUID = 1L;
    private transient JedisPool jedisPool; // Mark as transient to exclude from serialization
    private final String host;
    private final int port;

    public RedisSink(String host, int port) {
        this.host = host;
        this.port = port;
    }

    // Lazy initialization of JedisPool (not serialized)
    private JedisPool getJedisPool() {
        if (jedisPool == null) {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(10);
            poolConfig.setMaxIdle(5);
            poolConfig.setMinIdle(1);
            poolConfig.setTestOnBorrow(true);
            poolConfig.setBlockWhenExhausted(true);
            poolConfig.setMaxWaitMillis(5000);
            this.jedisPool = new JedisPool(poolConfig, host, port);
        }
        return jedisPool;
    }

    public void storeTaxiSpeed(com.taxifleet.models.TaxiSpeed speed) {
        try (Jedis jedis = getJedisPool().getResource()) {
            String taxiKey = "taxi:speed:" + speed.taxiId;
            jedis.hset(taxiKey, "speed", String.valueOf(speed.speed));
            jedis.hset(taxiKey, "distance", String.valueOf(speed.totalDistance));
            jedis.hset(taxiKey, "timestamp", String.valueOf(speed.timestamp));
            jedis.hset(taxiKey, "latitude", String.valueOf(speed.latitude));
            jedis.hset(taxiKey, "longitude", String.valueOf(speed.longitude));

            jedis.expire("taxi:speed:" + speed.taxiId, 60);
        } catch (Exception e) {
            System.err.println("Failed to store data in Redis: " + e.getMessage());
            e.printStackTrace();
        }
    }
}