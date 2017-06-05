package com.smart.redis;

import redis.clients.jedis.Jedis;

public abstract class JedisExecutor<T> {

    abstract T doInJedis(Jedis jedis);

}
