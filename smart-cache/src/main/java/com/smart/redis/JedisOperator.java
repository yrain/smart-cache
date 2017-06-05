package com.smart.redis;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.SortingParams;

/**
 * JedisOperator
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class JedisOperator {

    private JedisPool jedisPool;

    public JedisOperator() {
    }

    public JedisOperator(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // ---------------------------------------------------------------------------------------------------
    public <T> T execute(JedisExecutor<T> executor) {
        T result = null;
        Jedis jedis = jedisPool.getResource();
        try {
            result = executor.doInJedis(jedis);
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    public Jedis getJedis() {
        return jedisPool.getResource();
    }

    /**
     * https://github.com/xetorthio/jedis/blob/master/src/test/java/redis/clients/jedis/tests/commands/ScriptingCommandsTest.java
     */
    public Object eval(final String script) {
        return execute(new JedisExecutor<Object>() {
            @Override
            public Object doInJedis(Jedis jedis) {
                return jedis.eval(script);
            }
        });
    }

    public Object eval(final String script, final int keyCount, final String... params) {
        return execute(new JedisExecutor<Object>() {
            @Override
            public Object doInJedis(Jedis jedis) {
                return jedis.eval(script, keyCount, params);
            }
        });
    }

    public Object eval(final String script, final List<String> keys, final List<String> args) {
        return execute(new JedisExecutor<Object>() {
            @Override
            public Object doInJedis(Jedis jedis) {
                return jedis.eval(script, keys, args);
            }
        });
    }

    public Object eval(final byte[] script) {
        return execute(new JedisExecutor<Object>() {
            @Override
            public Object doInJedis(Jedis jedis) {
                return jedis.eval(script);
            }
        });
    }

    public Object eval(final byte[] script, final byte[] keyCount, final byte[]... params) {
        return execute(new JedisExecutor<Object>() {
            @Override
            public Object doInJedis(Jedis jedis) {
                return jedis.eval(script, keyCount, params);
            }
        });
    }

    public Object eval(final byte[] script, final int keyCount, final byte[]... params) {
        return execute(new JedisExecutor<Object>() {
            @Override
            public Object doInJedis(Jedis jedis) {
                return jedis.eval(script, keyCount, params);
            }
        });
    }

    public Object eval(final byte[] script, final List<byte[]> keys, final List<byte[]> args) {
        return execute(new JedisExecutor<Object>() {
            @Override
            public Object doInJedis(Jedis jedis) {
                return jedis.eval(script, keys, args);
            }
        });
    }

    //
    // string
    // ---------------------------------------------------------------------------------------------------
    public String set(final String key, final String value) {
        return execute(new JedisExecutor<String>() {
            @Override
            public String doInJedis(Jedis jedis) {
                return jedis.set(key, value);
            }
        });
    }

    public String get(final String key) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.get(key);
            }
        });
    }

    public Boolean exists(final String key) {
        return execute(new JedisExecutor<Boolean>() {
            @Override
            Boolean doInJedis(Jedis jedis) {
                return jedis.exists(key);
            }
        });
    }

    public String type(final String key) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.type(key);
            }
        });
    }

    public Long expire(final String key, final int seconds) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.expire(key, seconds);
            }
        });
    }

    public Long expireAt(final String key, final long unixTime) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.expireAt(key, unixTime);
            }
        });
    }

    public void flushAll() {
        execute(new JedisExecutor<Void>() {
            @Override
            public Void doInJedis(Jedis jedis) {
                jedis.flushAll();
                return null;
            }
        });
    }

    public Long ttl(final String key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.ttl(key);
            }
        });
    }

    public boolean setbit(final String key, final long offset, final boolean value) {
        return execute(new JedisExecutor<Boolean>() {
            @Override
            Boolean doInJedis(Jedis jedis) {
                return jedis.setbit(key, offset, value);
            }
        });
    }

    public boolean getbit(final String key, final long offset) {
        return execute(new JedisExecutor<Boolean>() {
            @Override
            Boolean doInJedis(Jedis jedis) {
                return jedis.getbit(key, offset);
            }
        });
    }

    public long setrange(final String key, final long offset, final String value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.setrange(key, offset, value);
            }
        });
    }

    public String getrange(final String key, final long startOffset, final long endOffset) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.getrange(key, startOffset, endOffset);
            }
        });
    }

    public String getSet(final String key, final String value) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.getSet(key, value);
            }
        });
    }

    public Long setnx(final String key, final String value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.setnx(key, value);
            }
        });
    }

    public String setex(final String key, final int seconds, final String value) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.setex(key, seconds, value);
            }
        });
    }

    public Set<String> keys(final String pattern) {
        return execute(new JedisExecutor<Set<String>>() {
            @Override
            Set<String> doInJedis(Jedis jedis) {
                return jedis.keys(pattern);
            }
        });
    }

    public List<String> mget(final String... keys) {
        return execute(new JedisExecutor<List<String>>() {
            @Override
            List<String> doInJedis(Jedis jedis) {
                return jedis.mget(keys);
            }
        });
    }

    public Long decrBy(final String key, final long value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.decrBy(key, value);
            }
        });
    }

    public Long decr(final String key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.decr(key);
            }
        });
    }

    public Long incrBy(final String key, final long value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.incrBy(key, value);
            }
        });
    }

    public Long incr(final String key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.incr(key);
            }
        });
    }

    public Long append(final String key, final String value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.append(key, value);
            }
        });
    }

    public String substr(final String key, final int start, final int end) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.substr(key, start, end);
            }
        });
    }

    public Long hset(final String key, final String field, final String value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.hset(key, field, value);
            }
        });
    }

    public String hget(final String key, final String field) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.hget(key, field);
            }
        });
    }

    public Long hsetnx(final String key, final String field, final String value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.hsetnx(key, field, value);
            }
        });
    }

    public String hmset(final String key, final Map<String, String> hash) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.hmset(key, hash);
            }
        });
    }

    public List<String> hmget(final String key, final String... fields) {
        return execute(new JedisExecutor<List<String>>() {
            @Override
            List<String> doInJedis(Jedis jedis) {
                return jedis.hmget(key, fields);
            }
        });
    }

    public Long hincrBy(final String key, final String field, final long value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.hincrBy(key, field, value);
            }
        });
    }

    public Boolean hexists(final String key, final String field) {
        return execute(new JedisExecutor<Boolean>() {
            @Override
            Boolean doInJedis(Jedis jedis) {
                return jedis.hexists(key, field);
            }
        });
    }

    public Long del(final String key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.del(key);
            }
        });
    }

    public String mset(final String... keyvalues) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.mset(keyvalues);
            }
        });
    }

    public Long mdel(final String... keys) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.del(keys);
            }
        });
    }

    public Long hdel(final String key, final String field) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.hdel(key, field);
            }
        });
    }

    public Long hmdel(final String key, final String... fields) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.hdel(key, fields);
            }
        });
    }

    public Long hlen(final String key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.hlen(key);
            }
        });
    }

    public Set<String> hkeys(final String key) {
        return execute(new JedisExecutor<Set<String>>() {
            @Override
            Set<String> doInJedis(Jedis jedis) {
                return jedis.hkeys(key);
            }
        });
    }

    public List<String> hvals(final String key) {
        return execute(new JedisExecutor<List<String>>() {
            @Override
            List<String> doInJedis(Jedis jedis) {
                return jedis.hvals(key);
            }
        });
    }

    public Map<String, String> hgetAll(final String key) {
        return execute(new JedisExecutor<Map<String, String>>() {
            @Override
            Map<String, String> doInJedis(Jedis jedis) {
                return jedis.hgetAll(key);
            }
        });
    }

    public Long rpush(final String key, final String string) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.rpush(key, string);
            }
        });
    }

    public Long lpush(final String key, final String string) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.lpush(key, string);
            }
        });
    }

    public Long llen(final String key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.llen(key);
            }
        });
    }

    public List<String> lrange(final String key, final long start, final long end) {
        return execute(new JedisExecutor<List<String>>() {
            @Override
            List<String> doInJedis(Jedis jedis) {
                return jedis.lrange(key, start, end);
            }
        });
    }

    public String ltrim(final String key, final long start, final long end) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.ltrim(key, start, end);
            }
        });
    }

    public String lindex(final String key, final long index) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.lindex(key, index);
            }
        });
    }

    public String lset(final String key, final long index, final String value) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.lset(key, index, value);
            }
        });
    }

    public Long lrem(final String key, final long count, final String value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.lrem(key, count, value);
            }
        });
    }

    public String lpop(final String key) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.lpop(key);
            }
        });
    }

    public String rpop(final String key) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.rpop(key);
            }
        });
    }

    // return 1 add a not exist value ,
    // return 0 add a exist value
    public Long sadd(final String key, final String member) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.sadd(key, member);
            }
        });
    }

    public Set<String> smembers(final String key) {
        return execute(new JedisExecutor<Set<String>>() {
            @Override
            Set<String> doInJedis(Jedis jedis) {
                return jedis.smembers(key);
            }
        });
    }

    public Long srem(final String key, final String member) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.srem(key, member);
            }
        });
    }

    public String spop(final String key) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.spop(key);
            }
        });
    }

    public Long scard(final String key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.scard(key);
            }
        });
    }

    public Boolean sismember(final String key, final String member) {
        return execute(new JedisExecutor<Boolean>() {
            @Override
            Boolean doInJedis(Jedis jedis) {
                return jedis.sismember(key, member);
            }
        });
    }

    public String srandmember(final String key) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.srandmember(key);
            }
        });
    }

    public Long zadd(final String key, final double score, final String member) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zadd(key, score, member);
            }
        });
    }

    public Set<String> zrange(final String key, final int start, final int end) {
        return execute(new JedisExecutor<Set<String>>() {
            @Override
            Set<String> doInJedis(Jedis jedis) {
                return jedis.zrange(key, start, end);
            }
        });
    }

    public Long zrem(final String key, final String member) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zrem(key, member);
            }
        });
    }

    public Double zincrby(final String key, final double score, final String member) {
        return execute(new JedisExecutor<Double>() {
            @Override
            Double doInJedis(Jedis jedis) {
                return jedis.zincrby(key, score, member);
            }
        });
    }

    public Long zrank(final String key, final String member) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zrank(key, member);
            }
        });
    }

    public Long zrevrank(final String key, final String member) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zrevrank(key, member);
            }
        });
    }

    public Set<String> zrevrange(final String key, final int start, final int end) {
        return execute(new JedisExecutor<Set<String>>() {
            @Override
            Set<String> doInJedis(Jedis jedis) {
                return jedis.zrevrange(key, start, end);
            }
        });
    }

    public Set<redis.clients.jedis.Tuple> zrangeWithScores(final String key, final int start, final int end) {
        return execute(new JedisExecutor<Set<redis.clients.jedis.Tuple>>() {
            @Override
            Set<redis.clients.jedis.Tuple> doInJedis(Jedis jedis) {
                return jedis.zrangeWithScores(key, start, end);
            }
        });
    }

    public Set<redis.clients.jedis.Tuple> zrevrangeWithScores(final String key, final int start, final int end) {
        return execute(new JedisExecutor<Set<redis.clients.jedis.Tuple>>() {
            @Override
            Set<redis.clients.jedis.Tuple> doInJedis(Jedis jedis) {
                return jedis.zrevrangeWithScores(key, start, end);
            }
        });
    }

    public Long zcard(final String key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zcard(key);
            }
        });
    }

    public Double zscore(final String key, final String member) {
        return execute(new JedisExecutor<Double>() {
            @Override
            Double doInJedis(Jedis jedis) {
                return jedis.zscore(key, member);
            }
        });
    }

    public List<String> sort(final String key) {
        return execute(new JedisExecutor<List<String>>() {
            @Override
            List<String> doInJedis(Jedis jedis) {
                return jedis.sort(key);
            }
        });
    }

    public List<String> sort(final String key, final SortingParams sortingParameters) {
        return execute(new JedisExecutor<List<String>>() {
            @Override
            List<String> doInJedis(Jedis jedis) {
                return jedis.sort(key, sortingParameters);
            }
        });
    }

    public Long zcount(final String key, final double min, final double max) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zcount(key, min, max);
            }
        });
    }

    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        return execute(new JedisExecutor<Set<String>>() {
            @Override
            Set<String> doInJedis(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max);
            }
        });
    }

    public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
        return execute(new JedisExecutor<Set<String>>() {
            @Override
            Set<String> doInJedis(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min);
            }
        });
    }

    public Set<String> zrangeByScore(final String key, final double min, final double max, final int offset, final int count) {
        return execute(new JedisExecutor<Set<String>>() {
            @Override
            Set<String> doInJedis(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max, offset, count);
            }
        });
    }

    public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset, final int count) {
        return execute(new JedisExecutor<Set<String>>() {
            @Override
            Set<String> doInJedis(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min, offset, count);
            }
        });
    }

    public Set<redis.clients.jedis.Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
        return execute(new JedisExecutor<Set<redis.clients.jedis.Tuple>>() {
            @Override
            Set<redis.clients.jedis.Tuple> doInJedis(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max);
            }
        });
    }

    public Set<redis.clients.jedis.Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
        return execute(new JedisExecutor<Set<redis.clients.jedis.Tuple>>() {
            @Override
            Set<redis.clients.jedis.Tuple> doInJedis(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min);
            }
        });
    }

    public Set<redis.clients.jedis.Tuple> zrangeByScoreWithScores(final String key, final double min, final double max, final int offset, final int count) {
        return execute(new JedisExecutor<Set<redis.clients.jedis.Tuple>>() {
            @Override
            Set<redis.clients.jedis.Tuple> doInJedis(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        });
    }

    public Set<redis.clients.jedis.Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min, final int offset, final int count) {
        return execute(new JedisExecutor<Set<redis.clients.jedis.Tuple>>() {
            @Override
            Set<redis.clients.jedis.Tuple> doInJedis(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        });
    }

    public Long zremrangeByRank(final String key, final int start, final int end) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zremrangeByRank(key, start, end);
            }
        });
    }

    public Long zremrangeByScore(final String key, final double start, final double end) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zremrangeByScore(key, start, end);
            }
        });
    }

    public Long linsert(final String key, final LIST_POSITION where, final String pivot, final String value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.linsert(key, where, pivot, value);
            }
        });
    }

    //
    // byte
    // ---------------------------------------------------------------------------------------------------
    public String set(final byte[] key, final byte[] value) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.set(key, value);
            }
        });
    }

    public byte[] get(final byte[] key) {
        return execute(new JedisExecutor<byte[]>() {
            @Override
            byte[] doInJedis(Jedis jedis) {
                return jedis.get(key);
            }
        });
    }

    public Long del(final byte[] key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.del(key);
            }
        });
    }

    public String mset(final byte[]... keyvalues) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.mset(keyvalues);
            }
        });
    }

    public Long mdel(final byte[]... keys) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.del(keys);
            }
        });
    }

    public Boolean exists(final byte[] key) {
        return execute(new JedisExecutor<Boolean>() {
            @Override
            Boolean doInJedis(Jedis jedis) {
                return jedis.exists(key);
            }
        });
    }

    public String type(final byte[] key) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.type(key);
            }
        });
    }

    public Long expire(final byte[] key, final int seconds) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.expire(key, seconds);
            }
        });
    }

    public Long expireAt(final byte[] key, final long unixTime) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.expireAt(key, unixTime);
            }
        });

    }

    /**
     * 以秒为单位，返回给定 key 的剩余生存时间
     * 不存在:-2
     * 永久:-1
     */
    public Long ttl(final byte[] key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.ttl(key);
            }
        });
    }

    public byte[] getSet(final byte[] key, final byte[] value) {
        return execute(new JedisExecutor<byte[]>() {
            @Override
            byte[] doInJedis(Jedis jedis) {
                return jedis.getSet(key, value);
            }
        });
    }

    public Long setnx(final byte[] key, final byte[] value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.setnx(key, value);
            }
        });
    }

    public String setex(final byte[] key, final int seconds, final byte[] value) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.setex(key, seconds, value);
            }
        });
    }

    public Set<byte[]> keys(final byte[] pattern) {
        return execute(new JedisExecutor<Set<byte[]>>() {
            @Override
            Set<byte[]> doInJedis(Jedis jedis) {
                return jedis.keys(pattern);
            }
        });
    }

    public List<byte[]> mget(final byte[]... keys) {
        return execute(new JedisExecutor<List<byte[]>>() {
            @Override
            List<byte[]> doInJedis(Jedis jedis) {
                return jedis.mget(keys);
            }
        });
    }

    public Long decrBy(final byte[] key, final long value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.decrBy(key, value);
            }
        });
    }

    public Long decr(final byte[] key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.decr(key);
            }
        });
    }

    public Long incrBy(final byte[] key, final long value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.incrBy(key, value);
            }
        });
    }

    public Long incr(final byte[] key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.incr(key);
            }
        });
    }

    public Long append(final byte[] key, final byte[] value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.append(key, value);
            }
        });
    }

    public byte[] substr(final byte[] key, final int start, final int end) {
        return execute(new JedisExecutor<byte[]>() {
            @Override
            byte[] doInJedis(Jedis jedis) {
                return jedis.substr(key, start, end);
            }
        });
    }

    //
    // hset
    // --------------------------------------------------------
    public Long hset(final byte[] key, final byte[] field, final byte[] value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.hset(key, field, value);
            }
        });
    }

    public byte[] hget(final byte[] key, final byte[] field) {
        return execute(new JedisExecutor<byte[]>() {
            @Override
            byte[] doInJedis(Jedis jedis) {
                return jedis.hget(key, field);
            }
        });
    }

    public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.hsetnx(key, field, value);
            }
        });
    }

    public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.hmset(key, hash);
            }
        });
    }

    public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
        return execute(new JedisExecutor<List<byte[]>>() {
            @Override
            List<byte[]> doInJedis(Jedis jedis) {
                return jedis.hmget(key, fields);
            }
        });
    }

    public Long hincrBy(final byte[] key, final byte[] field, final long value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.hincrBy(key, field, value);
            }
        });
    }

    public Boolean hexists(final byte[] key, final byte[] field) {
        return execute(new JedisExecutor<Boolean>() {
            @Override
            Boolean doInJedis(Jedis jedis) {
                return jedis.hexists(key, field);
            }
        });
    }

    public Long hdel(final byte[] key, final byte[] field) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.hdel(key, field);
            }
        });
    }

    public Long hmdel(final byte[] key, final byte[]... fields) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.hdel(key, fields);
            }
        });
    }

    public Long hlen(final byte[] key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.hlen(key);
            }
        });
    }

    public Set<byte[]> hkeys(final byte[] key) {
        return execute(new JedisExecutor<Set<byte[]>>() {
            @Override
            Set<byte[]> doInJedis(Jedis jedis) {
                return jedis.hkeys(key);
            }
        });
    }

    public Collection<byte[]> hvals(final byte[] key) {
        return execute(new JedisExecutor<Collection<byte[]>>() {
            @Override
            Collection<byte[]> doInJedis(Jedis jedis) {
                return jedis.hvals(key);
            }
        });
    }

    public Map<byte[], byte[]> hgetAll(final byte[] key) {
        return execute(new JedisExecutor<Map<byte[], byte[]>>() {
            @Override
            Map<byte[], byte[]> doInJedis(Jedis jedis) {
                return jedis.hgetAll(key);
            }
        });
    }

    //
    // list
    // ---------------------------------------------------------------------------------------------------
    public Long rpush(final byte[] key, final byte[] string) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.rpush(key, string);
            }
        });
    }

    public Long lpush(final byte[] key, final byte[] string) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.lpush(key, string);
            }
        });
    }

    public Long llen(final byte[] key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.llen(key);
            }
        });
    }

    public List<byte[]> lrange(final byte[] key, final int start, final int end) {
        return execute(new JedisExecutor<List<byte[]>>() {
            @Override
            List<byte[]> doInJedis(Jedis jedis) {
                return jedis.lrange(key, start, end);
            }
        });
    }

    public String ltrim(final byte[] key, final int start, final int end) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.ltrim(key, start, end);
            }
        });
    }

    public byte[] lindex(final byte[] key, final int index) {
        return execute(new JedisExecutor<byte[]>() {
            @Override
            byte[] doInJedis(Jedis jedis) {
                return jedis.lindex(key, index);
            }
        });
    }

    public String lset(final byte[] key, final int index, final byte[] value) {
        return execute(new JedisExecutor<String>() {
            @Override
            String doInJedis(Jedis jedis) {
                return jedis.lset(key, index, value);
            }
        });
    }

    public Long linsert(final byte[] key, final LIST_POSITION where, final byte[] pivot, final byte[] value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.linsert(key, where, pivot, value);
            }
        });
    }

    public Long lrem(final byte[] key, final int count, final byte[] value) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.lrem(key, count, value);
            }
        });
    }

    public byte[] lpop(final byte[] key) {
        return execute(new JedisExecutor<byte[]>() {
            @Override
            byte[] doInJedis(Jedis jedis) {
                return jedis.lpop(key);
            }
        });
    }

    public byte[] rpop(final byte[] key) {
        return execute(new JedisExecutor<byte[]>() {
            @Override
            byte[] doInJedis(Jedis jedis) {
                return jedis.rpop(key);
            }
        });
    }

    public byte[] rpoplpush(final byte[] srcKey, final byte[] dstKey) {
        return execute(new JedisExecutor<byte[]>() {
            @Override
            byte[] doInJedis(Jedis jedis) {
                return jedis.rpoplpush(srcKey, dstKey);
            }
        });
    }

    public byte[] brpoplpush(final byte[] srcKey, final byte[] dstKey, final int timeout) {
        return execute(new JedisExecutor<byte[]>() {
            @Override
            byte[] doInJedis(Jedis jedis) {
                return jedis.brpoplpush(srcKey, dstKey, timeout);
            }
        });
    }

    //
    // set
    // ---------------------------------------------------------------------------------------------------
    public Long sadd(final byte[] key, final byte[] member) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.sadd(key, member);
            }
        });
    }

    public Set<byte[]> smembers(final byte[] key) {
        return execute(new JedisExecutor<Set<byte[]>>() {
            @Override
            Set<byte[]> doInJedis(Jedis jedis) {
                return jedis.smembers(key);
            }
        });
    }

    public Long srem(final byte[] key, final byte[] member) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.srem(key, member);
            }
        });
    }

    public byte[] spop(final byte[] key) {
        return execute(new JedisExecutor<byte[]>() {
            @Override
            byte[] doInJedis(Jedis jedis) {
                return jedis.spop(key);
            }
        });
    }

    public Long scard(final byte[] key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.scard(key);
            }
        });
    }

    public Boolean sismember(final byte[] key, final byte[] member) {
        return execute(new JedisExecutor<Boolean>() {
            @Override
            Boolean doInJedis(Jedis jedis) {
                return jedis.sismember(key, member);
            }
        });
    }

    public byte[] srandmember(final byte[] key) {
        return execute(new JedisExecutor<byte[]>() {
            @Override
            byte[] doInJedis(Jedis jedis) {
                return jedis.srandmember(key);
            }
        });
    }

    //
    // SortedSet
    // ---------------------------------------------------------------------------------------------------
    public Long zadd(final byte[] key, final double score, final byte[] member) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zadd(key, score, member);
            }
        });
    }

    public Set<byte[]> zrange(final byte[] key, final int start, final int end) {
        return execute(new JedisExecutor<Set<byte[]>>() {
            @Override
            Set<byte[]> doInJedis(Jedis jedis) {
                return jedis.zrange(key, start, end);
            }
        });
    }

    public Long zrem(final byte[] key, final byte[] member) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zrem(key, member);
            }
        });
    }

    public Double zincrby(final byte[] key, final double score, final byte[] member) {
        return execute(new JedisExecutor<Double>() {
            @Override
            Double doInJedis(Jedis jedis) {
                return jedis.zincrby(key, score, member);
            }
        });
    }

    public Long zrank(final byte[] key, final byte[] member) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zrank(key, member);
            }
        });
    }

    public Long zrevrank(final byte[] key, final byte[] member) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zrevrank(key, member);
            }
        });
    }

    public Set<byte[]> zrevrange(final byte[] key, final int start, final int end) {
        return execute(new JedisExecutor<Set<byte[]>>() {
            @Override
            Set<byte[]> doInJedis(Jedis jedis) {
                return jedis.zrevrange(key, start, end);
            }
        });
    }

    public Set<redis.clients.jedis.Tuple> zrangeWithScores(final byte[] key, final int start, final int end) {
        return execute(new JedisExecutor<Set<redis.clients.jedis.Tuple>>() {
            @Override
            Set<redis.clients.jedis.Tuple> doInJedis(Jedis jedis) {
                return jedis.zrangeWithScores(key, start, end);
            }
        });
    }

    public Set<redis.clients.jedis.Tuple> zrevrangeWithScores(final byte[] key, final int start, final int end) {
        return execute(new JedisExecutor<Set<redis.clients.jedis.Tuple>>() {
            @Override
            Set<redis.clients.jedis.Tuple> doInJedis(Jedis jedis) {
                return jedis.zrevrangeWithScores(key, start, end);
            }
        });
    }

    public Long zcard(final byte[] key) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zcard(key);
            }
        });
    }

    public Double zscore(final byte[] key, final byte[] member) {
        return execute(new JedisExecutor<Double>() {
            @Override
            Double doInJedis(Jedis jedis) {
                return jedis.zscore(key, member);
            }
        });
    }

    public List<byte[]> sort(final byte[] key) {
        return execute(new JedisExecutor<List<byte[]>>() {
            @Override
            List<byte[]> doInJedis(Jedis jedis) {
                return jedis.sort(key);
            }
        });
    }

    public List<byte[]> sort(final byte[] key, final SortingParams sortingParameters) {
        return execute(new JedisExecutor<List<byte[]>>() {
            @Override
            List<byte[]> doInJedis(Jedis jedis) {
                return jedis.sort(key, sortingParameters);
            }
        });
    }

    public Long zcount(final byte[] key, final double min, final double max) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zcount(key, min, max);
            }
        });
    }

    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max) {
        return execute(new JedisExecutor<Set<byte[]>>() {
            @Override
            Set<byte[]> doInJedis(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max);
            }
        });
    }

    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max, final int offset, final int count) {
        return execute(new JedisExecutor<Set<byte[]>>() {
            @Override
            Set<byte[]> doInJedis(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max, offset, count);
            }
        });
    }

    public Set<redis.clients.jedis.Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {
        return execute(new JedisExecutor<Set<redis.clients.jedis.Tuple>>() {
            @Override
            Set<redis.clients.jedis.Tuple> doInJedis(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max);
            }
        });
    }

    public Set<redis.clients.jedis.Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max, final int offset, final int count) {
        return execute(new JedisExecutor<Set<redis.clients.jedis.Tuple>>() {
            @Override
            Set<redis.clients.jedis.Tuple> doInJedis(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        });
    }

    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min) {
        return execute(new JedisExecutor<Set<byte[]>>() {
            @Override
            Set<byte[]> doInJedis(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min);
            }
        });
    }

    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min, final int offset, final int count) {
        return execute(new JedisExecutor<Set<byte[]>>() {
            @Override
            Set<byte[]> doInJedis(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min, offset, count);
            }
        });
    }

    public Set<redis.clients.jedis.Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {
        return execute(new JedisExecutor<Set<redis.clients.jedis.Tuple>>() {
            @Override
            Set<redis.clients.jedis.Tuple> doInJedis(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min);
            }
        });
    }

    public Set<redis.clients.jedis.Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min, final int offset, final int count) {
        return execute(new JedisExecutor<Set<redis.clients.jedis.Tuple>>() {
            @Override
            Set<redis.clients.jedis.Tuple> doInJedis(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        });
    }

    public Long zremrangeByRank(final byte[] key, final int start, final int end) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zremrangeByRank(key, start, end);
            }
        });
    }

    public Long zremrangeByScore(final byte[] key, final double start, final double end) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.zremrangeByScore(key, start, end);
            }
        });
    }

    //
    // pubsub
    // ---------------------------------------------------------------------------------------------------
    public void subscribe(final JedisPubSub jedisPubSub, final String... channels) {
        execute(new JedisExecutor<Void>() {
            @Override
            Void doInJedis(Jedis jedis) {
                jedis.subscribe(jedisPubSub, channels);
                return null;
            }
        });
    }

    public void subscribe(final BinaryJedisPubSub jedisPubSub, final byte[]... channels) {
        execute(new JedisExecutor<Void>() {
            @Override
            Void doInJedis(Jedis jedis) {
                jedis.subscribe(jedisPubSub, channels);
                return null;
            }
        });
    }

    public Long publish(final String channel, final String message) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.publish(channel, message);
            }
        });
    }

    public Long publish(final byte[] channel, final byte[] message) {
        return execute(new JedisExecutor<Long>() {
            @Override
            Long doInJedis(Jedis jedis) {
                return jedis.publish(channel, message);
            }
        });
    }

    //
    // getter & setter
    // ---------------------------------------------------------------------------------------------------
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }
}
