package com.taxifleet.helper;

import com.taxifleet.models.TaxiSpeed;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;

public class RedisSink implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private transient JedisPool jedisPool;

    public RedisSink(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private JedisPool getPool() {
        if (jedisPool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(10);
            config.setMaxIdle(5);
            config.setMinIdle(1);
            config.setTestOnBorrow(true);
            jedisPool = new JedisPool(config, host, port);
        }
        return jedisPool;
    }

    public void store(TaxiSpeed speed) {
        try (Jedis jedis = getPool().getResource()) {
            String key = "taxi:speed:" + speed.taxiId;
            jedis.hset(key, "latitude",     String.valueOf(speed.latitude));
            jedis.hset(key, "longitude",    String.valueOf(speed.longitude));
            jedis.hset(key, "speed",        String.valueOf(speed.speed));
            jedis.hset(key, "distance",     String.valueOf(speed.totalDistance));
            jedis.hset(key, "timestamp",    String.valueOf(speed.timestamp));
            jedis.hset(key, "isSpeeding",   String.valueOf(speed.isSpeeding));
            jedis.hset(key, "isOutOfArea",  String.valueOf(speed.isOutOfArea));
            jedis.hset(key, "averageSpeed", String.valueOf(speed.averageSpeed));
            jedis.hset(key, "totalDistance",String.valueOf(speed.totalDistance));
            jedis.hset(key, "isParking",    String.valueOf(speed.isParking));
            jedis.hset(key, "lastMoved",    speed.lastMoved != null ? speed.lastMoved : "");
            jedis.expire(key, 60);
        } catch (Exception e) {
            System.err.println("Redis write failed for taxi " + speed.taxiId + ": " + e.getMessage());
        }
    }
}
