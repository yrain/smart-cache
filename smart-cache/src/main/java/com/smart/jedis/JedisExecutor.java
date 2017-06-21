package com.smart.jedis;

import redis.clients.jedis.Jedis;

/**
 * JedisExecutor
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public abstract class JedisExecutor<T> {

    abstract T doInJedis(Jedis jedis);

}
