package com.smart.jedis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.InitializingBean;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.smart.serializer.FSTSerializer;
import com.smart.serializer.Serializer;
import com.smart.serializer.StringSerializer;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.SortingParams;

/**
 * JedisTemplate
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class JedisTemplate implements InitializingBean {

    private JedisPool     jedisPool;
    private JedisOperator jedisOperator;
    private JedisCluster  jedisCluster;
    private boolean       cluster          = false;
    private Serializer    keySerializer;
    private Serializer    valSerializer;
    private Serializer    stringSerializer = new StringSerializer();

    public JedisTemplate() {
    }

    public JedisTemplate(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public JedisTemplate(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.jedisCluster != null) {
            this.cluster = true;
        } else {
            if (this.jedisPool != null) {
                this.jedisOperator = new JedisOperator(this.jedisPool);
            }
        }
        if (null == keySerializer) {
            this.keySerializer = this.stringSerializer;
        }
        if (null == valSerializer) {
            this.valSerializer = new FSTSerializer();
        }
    }

    // ---------------------------------------------------------------------------------------------------
    /**
     * 设值-对象
     */
    public boolean set(Object key, Object value) {
        if (null == key || null == value) {
            return false;
        }
        if (cluster) {
            return jedisCluster.set(serializeKey(key), serializeVal(value)).equals("OK");
        } else {
            return jedisOperator.set(serializeKey(key), serializeVal(value)).equals("OK");
        }
    }

    /**
     * 设值-对象-有效时间
     */
    public boolean set(Object key, Object value, int seconds) {
        boolean result = false;
        if (null == key || null == value) {
            return result;
        }
        if (cluster) {
            return jedisCluster.setex(serializeKey(key), seconds, serializeVal(value)).equals("OK");
        } else {
            return jedisOperator.setex(serializeKey(key), seconds, serializeVal(value)).equals("OK");
        }
    }

    /**
     * 取值-对象
     * 如果 key 不存在那么返回特殊值 nil 。
     * 假如 key 储存的值不是字符串类型，返回一个错误，因为 GET 只能用于处理字符串值。
     * 当 key 不存在时，返回 nil ，否则，返回 key 的值。
     * 如果 key 不是字符串类型，那么返回一个错误。
     */
    @SuppressWarnings("unchecked")
    public <E> E get(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return (E) deserializeVal(jedisCluster.get(serializeKey(key)));
        } else {
            return (E) deserializeVal(jedisOperator.get(serializeKey(key)));
        }
    }

    /**
     * 设值-integer
     */
    public boolean integer(Object key, Object value) {
        if (null == key || null == value) {
            return false;
        }
        if (cluster) {
            return jedisCluster.set(stringSerializer.serialize(key), stringSerializer.serialize(value)).equals("OK");
        } else {
            return jedisOperator.set(stringSerializer.serialize(key), stringSerializer.serialize(value)).equals("OK");
        }
    }

    /**
     * 取值-integer
     */
    @SuppressWarnings("unchecked")
    public <E> E integer(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return (E) stringSerializer.deserialize(jedisCluster.get(stringSerializer.serialize(key)));
        } else {
            return (E) stringSerializer.deserialize(jedisOperator.get(stringSerializer.serialize(key)));
        }
    }

    /**
     * 设值-string
     */
    public boolean string(Object key, Object value) {
        if (null == key || null == value) {
            return false;
        }
        if (cluster) {
            return jedisCluster.set(stringSerializer.serialize(key), stringSerializer.serialize(value)).equals("OK");
        } else {
            return jedisOperator.set(stringSerializer.serialize(key), stringSerializer.serialize(value)).equals("OK");
        }
    }

    /**
     * 取值-string
     */
    @SuppressWarnings("unchecked")
    public <E> E string(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return (E) stringSerializer.deserialize(jedisCluster.get(stringSerializer.serialize(key)));
        } else {
            return (E) stringSerializer.deserialize(jedisOperator.get(stringSerializer.serialize(key)));
        }
    }

    /**
     * 判断Key是否存在
     */
    public Boolean exists(Object key) {
        if (null == key) {
            return false;
        }
        if (cluster) {
            return jedisCluster.exists(serializeKey(key));
        } else {
            return jedisOperator.exists(serializeKey(key));
        }
    }

    /**
     * 删除给定的一个或多个 key 。
     * 不存在的 key 会被忽略。
     */
    public boolean del(Object key) {
        if (null == key) {
            return false;
        }
        if (cluster) {
            return jedisCluster.del(serializeKey(key)) != 0;
        } else {
            return jedisOperator.del(serializeKey(key)) != 0;
        }
    }

    /**
     * 返回 key 所储存的值的类型
     */
    public String type(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.type(serializeKey(key));
        } else {
            return jedisOperator.type(serializeKey(key));
        }
    }

    /**
     * 为给定 key 设置生存时间，当 key 过期时(生存时间为 0 )，它会被自动删除。
     */
    public Long expire(Object key, int seconds) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.expire(serializeKey(key), seconds);
        } else {
            return jedisOperator.expire(serializeKey(key), seconds);
        }
    }

    /**
     * EXPIREAT 的作用和 EXPIRE 类似，都用于为 key 设置生存时间。
     * 不同在于 EXPIREAT 命令接受的时间参数是 UNIX 时间戳(unix timestamp)。
     */
    public Long expireAt(Object key, long unixTime) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.expireAt(serializeKey(key), unixTime);
        } else {
            return jedisOperator.expireAt(serializeKey(key), unixTime);
        }
    }

    /**
     * 以秒为单位，返回给定 key 的剩余生存时间
     * 不存在:-2
     * 永久:-1
     * 修改为:
     * 永久:0
     * 不存在:-1
     */
    public Long ttl(Object key) {
        if (null == key) {
            return null;
        }
        long ttl = -2;
        if (cluster) {
            ttl = jedisCluster.ttl(serializeKey(key));
        } else {
            ttl = jedisOperator.ttl(serializeKey(key));
        }
        // 当 key 不存在时，返回 -2 。
        if (ttl == -2) {
            return -1l;
        }
        // 当 key 存在但没有设置剩余生存时间时，返回 -1
        if (ttl == -1) {
            return 0l;
        }
        return ttl;
    }

    /**
     * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)。
     * 当 key 存在但不是字符串类型时，返回一个错误。
     * 返回给定 key 的旧值。
     * 当 key 没有旧值时，也即是， key 不存在时，返回 nil 。
     */
    @SuppressWarnings("unchecked")
    public <E> E getSet(Object key, Object value) {
        if (null == key || null == value) {
            return null;
        }
        if (cluster) {
            return (E) deserializeVal(jedisCluster.getSet(serializeKey(key), serializeVal(value)));
        } else {
            return (E) deserializeVal(jedisOperator.getSet(serializeKey(key), serializeVal(value)));
        }
    }

    /**
     * 取值.删除
     */
    public <E> E getDel(Object key) {
        if (null == key) {
            return null;
        }
        E value = this.get(key);
        this.del(key);
        return value;
    }

    /**
     * 将 key 的值设为 value ，当且仅当 key 不存在。
     * 若给定的 key 已经存在，则 SETNX 不做任何动作。
     * SETNX 是『SET if Not eXists』(如果不存在，则 SET)的简写。
     */
    public boolean setnx(Object key, Object value) {
        if (null == key || null == value) {
            return false;
        }
        if (cluster) {
            return jedisCluster.setnx(serializeKey(key), serializeVal(value)) == 1;
        } else {
            return jedisOperator.setnx(serializeKey(key), serializeVal(value)) == 1;
        }
    }

    /**
     * 清空所有
     */
    @SuppressWarnings("deprecation")
    public void flushAll() {
        if (cluster) {
            jedisCluster.flushAll();
        } else {
            jedisOperator.flushAll();
        }
    }

    /**
     * 设值-多个
     * Logs.msg(jedisTemplate.mset("x", User.I, "y", User.I, "z", User.I));
     * Logs.msg(jedisTemplate.mget("x", "y", "z"));
     */
    public boolean mset(Object... keyvalues) {
        if (null == keyvalues || keyvalues.length == 0) {
            return false;
        }
        List<byte[]> args = Lists.newArrayList();
        for (int i = 0; i < keyvalues.length; i++) {
            if (i % 2 == 0) {
                args.add(serializeKey(keyvalues[i]));
            } else {
                args.add(serializeVal(keyvalues[i]));
            }
        }
        if (cluster) {
            return jedisCluster.mset(convertByteListToByteArray(args)).equals("OK");
        } else {
            return jedisOperator.mset(convertByteListToByteArray(args)).equals("OK");
        }
    }

    public boolean mset(List<Object> keyvalues) {
        return this.mset(keyvalues.toArray());
    }

    /**
     * 根据keys获取多个
     */
    public <E> List<E> mget(List<?> keys) {
        return this.mget(keys.toArray());
    }

    /**
     * 根据keys获取多个
     */
    private <E> List<E> mget(Object... keys) {
        if (cluster) {
            return convertBytesCollectionToList_deserializeVal(jedisCluster.mget(convertObjectArrayToByteArray_serializeKey(keys)));
        } else {
            return convertBytesCollectionToList_deserializeVal(jedisOperator.mget(convertObjectArrayToByteArray_serializeKey(keys)));
        }
    }

    /**
     * 根据keys获取多个
     */
    public <E> List<E> mget(String... keys) {
        if (cluster) {
            return convertBytesCollectionToList_deserializeVal(jedisCluster.mget(convertObjectArrayToByteArray_serializeKey(keys)));
        } else {
            return convertBytesCollectionToList_deserializeVal(jedisOperator.mget(convertObjectArrayToByteArray_serializeKey(keys)));
        }
    }

    /**
     * 删除-多个
     */
    public boolean mdel(List<?> keys) {
        return this.mdel(keys.toArray());
    }

    /**
     * 删除-多个
     */
    private boolean mdel(Object... keys) {
        if (null == keys || keys.length == 0) {
            return false;
        }
        if (cluster) {
            return jedisCluster.del(convertObjectArrayToByteArray_serializeKey(keys)) != 0;
        } else {
            return jedisOperator.mdel(convertObjectArrayToByteArray_serializeKey(keys)) != 0;
        }
    }

    /**
     * 删除-多个
     */
    public boolean mdel(String... keys) {
        if (null == keys || keys.length == 0) {
            return false;
        }
        if (cluster) {
            return jedisCluster.del(convertObjectArrayToByteArray_serializeKey(keys)) != 0;
        } else {
            return jedisOperator.mdel(convertObjectArrayToByteArray_serializeKey(keys)) != 0;
        }
    }

    //
    // int
    // ---------------------------------------------------------------------------------------------------
    /**
     * 将 key 中储存的数字值 +1
     * 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作。
     * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
     * 返回值：执行 INCR 命令之后 key 的值。
     */
    public Long incr(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.incr(serializeKey(key));
        } else {
            return jedisOperator.incr(serializeKey(key));
        }
    }

    /**
     * 将 key 所储存的值加上增量 increment 。
     * 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCRBY 命令。
     * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
     * 返回值：加上 increment 之后， key 的值。
     */
    public Long incrBy(Object key, long value) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.incrBy(serializeKey(key), value);
        } else {
            return jedisOperator.incrBy(serializeKey(key), value);
        }
    }

    /**
     * 将 key 中储存的数字值减一。
     * 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECR 操作。
     * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
     * 返回值：执行 DECR 命令之后 key 的值。
     */
    public Long decr(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.decr(serializeKey(key));
        } else {
            return jedisOperator.decr(serializeKey(key));
        }
    }

    /**
     * 将 key 所储存的值减去减量 decrement 。
     * 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECRBY 操作。
     * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
     * 返回值：减去 decrement 之后， key 的值。
     */
    public Long decrBy(Object key, long value) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.decrBy(serializeKey(key), value);
        } else {
            return jedisOperator.decrBy(serializeKey(key), value);
        }
    }

    //
    // hash
    // ---------------------------------------------------------------------------------------------------
    /**
     * hash.设值
     * 将哈希表 key 中的域 field 的值设为 value 。
     * 如果 key 不存在，一个新的哈希表被创建并进行 HSET 操作。
     * 如果域 field 已经存在于哈希表中，旧值将被覆盖。
     * 返回值：
     * 如果 field 是哈希表中的一个新建域，并且值设置成功，返回 1 。
     * 如果哈希表中域 field 已经存在且旧值已被新值覆盖，返回 0 。
     */
    public Long hset(Object key, Object field, Object value) {
        if (null == key || null == field || null == value) {
            return null;
        }
        if (cluster) {
            return jedisCluster.hset(serializeKey(key), serializeKey(field), serializeVal(value));
        } else {
            return jedisOperator.hset(serializeKey(key), serializeKey(field), serializeVal(value));
        }
    }

    /**
     * hash.设值.有效时间
     */
    public Long hset(Object key, Object field, Object value, int seconds) {
        if (null == key || null == field || null == value) {
            return null;
        }
        Long result = this.hset(key, field, value);
        this.expire(key, seconds);
        return result;
    }

    /**
     * hash.设值
     * 如果key不存在,则新建.返回1
     * 如果key存在,则取消新建,返回0
     */
    public boolean hsetnx(Object key, Object field, Object value) {
        if (null == key || null == field || null == value) {
            return false;
        }
        if (cluster) {
            return jedisCluster.hsetnx(serializeKey(key), serializeKey(field), serializeVal(value)) == 1;
        } else {
            return jedisOperator.hsetnx(serializeKey(key), serializeKey(field), serializeVal(value)) == 1;
        }
    }

    /**
     * hash.取值
     * 返回哈希表 key 中给定域 field 的值。
     * 返回值：
     * 给定域的值。
     * 当给定域不存在或是给定 key 不存在时，返回 nil 。
     */
    @SuppressWarnings("unchecked")
    public <E> E hget(Object key, Object field) {
        if (null == key || null == field) {
            return null;
        }
        if (cluster) {
            return (E) deserializeVal(jedisCluster.hget(serializeKey(key), serializeKey(field)));
        } else {
            return (E) deserializeVal(jedisOperator.hget(serializeKey(key), serializeKey(field)));
        }
    }

    /**
     * hash.取值.多个
     */
    @SuppressWarnings("unchecked")
    public <E> List<E> hget(Object key, List<Object> fields) {
        if (null == key || null == fields) {
            return null;
        }
        List<E> datas = Lists.newArrayList();
        for (Object field : fields) {
            datas.add((E) this.hget(key, field));
        }
        return datas;
    }

    /**
     * 返回哈希表 key 中，所有的域和值。
     * 在返回值里，紧跟每个域名(field name)之后是域的值(value)，所以返回值的长度是哈希表大小的两倍。
     * 返回值：
     * 以列表形式返回哈希表的域和域的值。
     * 若 key 不存在，返回空列表。
     */
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> hgetAll(String key) {
        if (null == key) {
            return null;
        }
        Map<K, V> data = Maps.newHashMap();
        Map<byte[], byte[]> bytes = null;
        if (cluster) {
            bytes = jedisCluster.hgetAll(serializeKey(key));
        } else {
            bytes = jedisOperator.hgetAll(serializeKey(key));
        }
        for (byte[] k : bytes.keySet()) {
            data.put((K) deserializeKey(k), (V) deserializeVal(bytes.get(k)));
        }
        return data;
    }

    /**
     * hash.取值.并删除
     */
    public <E> E hgetAndDel(Object key, Object field) {
        if (null == key || null == field) {
            return null;
        }
        E value = this.hget(key, field);
        this.hdel(key, field);
        return value;
    }

    /**
     * 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略。
     * 返回值:被成功移除的域的数量，不包括被忽略的域。
     */
    public boolean hdel(Object key, Object field) {
        if (null == key || null == field) {
            return false;
        }
        if (cluster) {
            return jedisCluster.hdel(serializeKey(key), serializeKey(field)) != 0;
        } else {
            return jedisOperator.hdel(serializeKey(key), serializeKey(field)) != 0;
        }
    }

    /**
     * 判断哈希表 key 中，指定 field 是否存在。
     */
    public boolean hexists(Object key, Object field) {
        if (null == key || null == field) {
            return false;
        }
        if (cluster) {
            return jedisCluster.hexists(serializeKey(key), serializeKey(field));
        } else {
            return jedisOperator.hexists(serializeKey(key), serializeKey(field));
        }
    }

    /**
     * 返回哈希表 key 中 field 的数量。
     * 返回值：
     * 哈希表中域的数量。
     * 当 key 不存在时，返回 0
     */
    public Long hlen(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.hlen(serializeKey(key));
        } else {
            return jedisOperator.hlen(serializeKey(key));
        }
    }

    /**
     * 返回哈希表 key 中的所有域。
     * 返回值：
     * 一个包含哈希表中所有域的表。
     * 当 key 不存在时，返回一个空表。
     */
    @SuppressWarnings("unchecked")
    public <E> Set<E> hkeys(Object key) {
        if (null == key) {
            return null;
        }
        Set<E> result = Sets.newHashSet();
        Set<byte[]> bytes = null;
        if (cluster) {
            bytes = jedisCluster.hkeys(serializeKey(key));
        } else {
            bytes = jedisOperator.hkeys(serializeKey(key));
        }
        for (byte[] bs : bytes) {
            result.add((E) deserializeKey(bs));
        }
        return result;
    }

    /**
     * 返回哈希表 key 中所有域的值。
     * 返回值：
     * 一个包含哈希表中所有值的表。
     * 当 key 不存在时，返回一个空表。
     */
    public <E> List<E> hvals(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertBytesCollectionToList_deserializeVal(jedisCluster.hvals(serializeKey(key)));
        } else {
            return convertBytesCollectionToList_deserializeVal(jedisOperator.hvals(serializeKey(key)));
        }
    }

    /**
     * 同时将多个 field-value (域-值)对设置到哈希表 key 中。
     * 此命令会覆盖哈希表中已存在的域。
     * 如果 key 不存在，一个空哈希表被创建并执行 HMSET 操作。
     */
    public boolean hmset(Object key, Map<?, ?> hash) {
        if (null == key || null == hash) {
            return false;
        }
        Map<byte[], byte[]> map = Maps.newHashMap();
        for (Object field : hash.keySet()) {
            map.put(serializeKey(field), serializeVal(hash.get(field)));
        }
        if (cluster) {
            return jedisCluster.hmset(serializeKey(key), map).equals("OK");
        } else {
            return jedisOperator.hmset(serializeKey(key), map).equals("OK");
        }
    }

    /**
     * 根据key和field,同时获取多个值
     */
    public <E> List<E> hmget(Object key, List<Object> fields) {
        return this.hmget(key, fields.toArray());
    }

    /**
     * 根据key和field,同时获取多个值
     */
    @SuppressWarnings("unchecked")
    public <E> List<E> hmget(Object key, Object... fields) {
        if (null == key) {
            return null;
        }
        List<E> objs = Lists.newArrayList();
        if (fields.length <= 0) {
            fields = hkeys(key).toArray();
        }
        List<byte[]> bytes = null;
        if (cluster) {
            bytes = jedisCluster.hmget(serializeKey(key), convertObjectArrayToByteArray_serializeKey(fields));
        } else {
            bytes = jedisOperator.hmget(serializeKey(key), convertObjectArrayToByteArray_serializeKey(fields));
        }
        for (byte[] bs : bytes) {
            objs.add((E) deserializeVal(bs));
        }
        return objs;
    }

    /**
     * hash.删除多个
     */
    public boolean hmdel(Object key, List<Object> fields) {
        return this.hmdel(key, fields.toArray());
    }

    /**
     * hash.删除多个
     */
    public boolean hmdel(Object key, Object... fields) {
        if (null == key || null == fields || fields.length == 0) {
            return false;
        }
        if (cluster) {
            return jedisCluster.hdel(serializeKey(key), convertObjectArrayToByteArray_serializeKey(fields)) != 0;
        } else {
            return jedisOperator.hmdel(serializeKey(key), convertObjectArrayToByteArray_serializeKey(fields)) != 0;
        }
    }

    /**
     * 为哈希表 key 中的 field 的值加上增量 increment 。
     * 增量也可以为负数，相当于对给定域进行减法操作。
     * 如果 key 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。
     * 如果域 field 不存在，那么在执行命令前，域的值被初始化为 0 。
     * 对一个储存字符串值的域 field 执行 HINCRBY 命令将造成一个错误。
     * 返回值：执行 HINCRBY 命令之后，哈希表 key 中域 field 的值。
     */
    public Long hincrBy(Object key, Object field, long value) {
        if (null == key || null == field) {
            return null;
        }
        if (cluster) {
            return jedisCluster.hincrBy(serializeKey(key), serializeKey(field), value);
        } else {
            return jedisOperator.hincrBy(serializeKey(key), serializeKey(field), value);
        }
    }

    //
    // list
    // ---------------------------------------------------------------------------------------------------

    /**
     * 获取list中所有元素
     */
    public <E> List<E> lgetAll(Object key) {
        return this.lrange(key, 0, -1);
    }

    /**
     * 将一个或多个值 value 插入到列表 key 的表头
     * 如果有多个 value 值，那么各个 value 值按从左到右的顺序依次插入到表头： 比如说，对空列表 mylist 执行命令 LPUSH mylist a b c ，列表的值将是 c b a ，这等同于原子性地执行 LPUSH mylist a 、 LPUSH mylist b 和 LPUSH mylist c 三个命令。
     * 如果 key 不存在，一个空列表会被创建并执行 LPUSH 操作。
     * 当 key 存在但不是列表类型时，返回一个错误。
     */
    public Long lpush(Object key, Object obj) {
        if (null == key || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.lpush(serializeKey(key), serializeVal(obj));
        } else {
            return jedisOperator.lpush(serializeKey(key), serializeVal(obj));
        }
    }

    /**
     * 移除并返回列表 key 的头元素。
     */
    @SuppressWarnings("unchecked")
    public <E> E lpop(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return (E) deserializeVal(jedisCluster.lpop(serializeKey(key)));
        } else {
            return (E) deserializeVal(jedisOperator.lpop(serializeKey(key)));
        }
    }

    /**
     * 将列表 key 下标为 index 的元素的值设置为 value 。
     * 当 index 参数超出范围，或对一个空列表( key 不存在)进行 LSET 时，返回一个错误。
     * 关于列表下标的更多信息，请参考 LINDEX 命令。
     */
    public boolean lset(Object key, Object obj, int index) {
        if (null == key || null == obj) {
            return false;
        }
        if (cluster) {
            return jedisCluster.lset(serializeKey(key), index, serializeVal(obj)).equals("OK");
        } else {
            return jedisOperator.lset(serializeKey(key), index, serializeVal(obj)).equals("OK");
        }
    }

    /**
     * 将值 value 插入到列表 key 当中，位于值 pivot 之前或之后。
     * 当 pivot 不存在于列表 key 时，不执行任何操作。
     * 当 key 不存在时， key 被视为空列表，不执行任何操作。
     * 如果 key 不是列表类型，返回一个错误。
     */
    public Long linsert(Object key, LIST_POSITION where, Object pivot, Object obj) {
        if (null == key || null == pivot || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.linsert(serializeKey(key), where, serializeVal(pivot), serializeVal(obj));
        } else {
            return jedisOperator.linsert(serializeKey(key), where, serializeVal(pivot), serializeVal(obj));
        }
    }

    /**
     * 批量移除
     */
    public void batchlrem(Object key, List<byte[]> vals) {
        for (byte[] val : vals) {
            this.lrem(key, -1, val);
        }
    }

    /**
     * 根据参数 count 的值，移除列表中与参数 value 相等的元素。
     * count 的值可以是以下几种：
     * count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count 。
     * count < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值。
     * count = 0 : 移除表中所有与 value 相等的值。
     */
    public Long lrem(Object key, int count, Object obj) {
        if (null == key || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.lrem(serializeKey(key), count, serializeVal(obj));
        } else {
            return jedisOperator.lrem(serializeKey(key), count, serializeVal(obj));
        }
    }

    public Long lrem(Object key, Object obj) {
        if (null == key || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.lrem(serializeKey(key), 0, serializeVal(obj));
        } else {
            return jedisOperator.lrem(serializeKey(key), 0, serializeVal(obj));
        }
    }

    /**
     * 返回列表 key 的长度。
     * 如果 key 不存在，则 key 被解释为一个空列表，返回 0 .
     * 如果 key 不是列表类型，返回一个错误。
     */
    public Long llen(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.llen(serializeKey(key));
        } else {
            return jedisOperator.llen(serializeKey(key));
        }
    }

    /**
     * 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 stop 指定。
     * 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
     * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
     * 注意LRANGE命令和编程语言区间函数的区别
     * 假如你有一个包含一百个元素的列表，对该列表执行 LRANGE list 0 10 ，结果是一个包含11个元素的列表，这表明 stop 下标也在 LRANGE 命令的取值范围之内(闭区间)，这和某些语言的区间函数可能不一致，比如Ruby的 Range.new 、 Array#slice 和Python的 range() 函数。
     * 超出范围的下标
     * 超出范围的下标值不会引起错误。
     * 如果 start 下标比列表的最大下标 end ( LLEN list 减去 1 )还要大，那么 LRANGE 返回一个空列表。
     * 如果 stop 下标比 end 下标还要大，Redis将 stop 的值设置为 end 。
     */
    public <E> List<E> lrange(Object key, int start, int end) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertBytesCollectionToList_deserializeVal(jedisCluster.lrange(serializeKey(key), start, end));
        } else {
            return convertBytesCollectionToList_deserializeVal(jedisOperator.lrange(serializeKey(key), start, end));
        }
    }

    /**
     * 对一个列表进行修剪(trim)，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
     * 举个例子，执行命令 LTRIM list 0 2 ，表示只保留列表 list 的前三个元素，其余元素全部删除。
     * 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
     * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
     * 当 key 不是列表类型时，返回一个错误。
     */
    public boolean ltrim(Object key, int start, int end) {
        if (null == key) {
            return false;
        }
        if (cluster) {
            return jedisCluster.ltrim(serializeKey(key), start, end).equals("OK");
        } else {
            return jedisOperator.ltrim(serializeKey(key), start, end).equals("OK");
        }
    }

    /**
     * 返回列表 key 中，下标为 index 的元素。
     * 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
     * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
     * 如果 key 不是列表类型，返回一个错误。
     */
    @SuppressWarnings("unchecked")
    public <E> E lindex(Object key, int index) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return (E) deserializeVal(jedisCluster.lindex(serializeKey(key), index));
        } else {
            return (E) deserializeVal(jedisOperator.lindex(serializeKey(key), index));
        }
    }

    /**
     * 将一个或多个值 value 插入到列表 key 的表尾(最右边)。
     * 如果有多个 value 值，那么各个 value 值按从左到右的顺序依次插入到表尾：比如对一个空列表 mylist 执行 RPUSH mylist a b c ，得出的结果列表为 a b c ，等同于执行命令 RPUSH mylist a 、 RPUSH mylist b 、 RPUSH mylist c 。
     * 如果 key 不存在，一个空列表会被创建并执行 RPUSH 操作。
     * 当 key 存在但不是列表类型时，返回一个错误。
     */
    public Long rpush(Object key, Object obj) {
        if (null == key || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.rpush(serializeKey(key), serializeVal(obj));
        } else {
            return jedisOperator.rpush(serializeKey(key), serializeVal(obj));
        }
    }

    /**
     * 移除并返回列表 key 的尾元素。
     */
    @SuppressWarnings("unchecked")
    public <E> E rpop(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return (E) deserializeVal(jedisCluster.rpop(serializeKey(key)));
        } else {
            return (E) deserializeVal(jedisOperator.rpop(serializeKey(key)));
        }
    }

    /**
     * 一个原子时间内，执行以下两个动作:
     * 将列表 source 中的最后一个元素(尾元素)弹出，并返回给客户端
     * 将 source 弹出的元素插入到列表 destination ，作为 destination 列表的的头元素
     * 
     * @param srcKey
     * @param dstKey
     * @return
     */
    public byte[] rpoplpush(Object srcKey, Object dstKey) {
        if (null == srcKey || null == dstKey) {
            return null;
        }
        if (cluster) {
            return jedisCluster.rpoplpush(serializeKey(srcKey), serializeKey(dstKey));
        } else {
            return jedisOperator.rpoplpush(serializeKey(srcKey), serializeKey(dstKey));
        }
    }

    /**
     * 是 rpoplpush 的阻塞版本，当给定列表 source 不为空时， BRPOPLPUSH 的表现和 RPOPLPUSH 一样
     * 
     * @param srcKey
     * @param dstKey
     * @param timeout
     * @return
     */
    public byte[] brpoplpush(Object srcKey, Object dstKey, int timeout) {
        if (null == srcKey || null == dstKey) {
            return null;
        }
        if (cluster) {
            return jedisCluster.brpoplpush(serializeKey(srcKey), serializeKey(dstKey), timeout);
        } else {
            return jedisOperator.brpoplpush(serializeKey(srcKey), serializeKey(dstKey), timeout);
        }
    }

    //
    // set
    // ---------------------------------------------------------------------------------------------------
    /**
     * 将一个或多个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略。
     * 假如 key 不存在，则创建一个只包含 member 元素作成员的集合。
     * 当 key 不是集合类型时，返回一个错误。
     */
    public Long sadd(Object key, Object obj) {
        if (null == key || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.sadd(serializeKey(key), serializeVal(obj));
        } else {
            return jedisOperator.sadd(serializeKey(key), serializeVal(obj));
        }
    }

    public Long sadd(Object key, Object obj, int seconds) {
        if (null == key || null == obj) {
            return null;
        }
        Long result = this.sadd(key, obj);
        this.expire(key, seconds);
        return result;
    }

    /**
     * 返回集合 key 中的所有成员。
     * 不存在的 key 被视为空集合。
     */
    public <E> Set<E> smembers(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertBytesCollectionToSet_deserializeVal(jedisCluster.smembers(serializeKey(key)));
        } else {
            return convertBytesCollectionToSet_deserializeVal(jedisOperator.smembers(serializeKey(key)));
        }

    }

    /**
     * 移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略。
     * 当 key 不是集合类型，返回一个错误。
     */
    public Long srem(Object key, Object obj) {
        if (null == key || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.srem(serializeKey(key), serializeVal(obj));
        } else {
            return jedisOperator.srem(serializeKey(key), serializeVal(obj));
        }
    }

    /**
     * 移除并返回集合中的一个随机元素。
     * 如果只想获取一个随机元素，但不想该元素从集合中被移除的话，可以使用 SRANDMEMBER 命令。
     */
    @SuppressWarnings("unchecked")
    public <E> E spop(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return (E) deserializeVal(jedisCluster.spop(serializeKey(key)));
        } else {
            return (E) deserializeVal(jedisOperator.spop(serializeKey(key)));
        }
    }

    /**
     * 如果命令执行时，只提供了 key 参数，那么返回集合中的一个随机元素。
     * 从 Redis 2.6 版本开始， SRANDMEMBER 命令接受可选的 count 参数：
     * 如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。如果 count 大于等于集合基数，那么返回整个集合。
     * 如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值。
     * 该操作和 SPOP 相似，但 SPOP 将随机元素从集合中移除并返回，而 SRANDMEMBER 则仅仅返回随机元素，而不对集合进行任何改动。
     */
    @SuppressWarnings("unchecked")
    public <E> E srandmember(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return (E) deserializeVal(jedisCluster.srandmember(serializeKey(key)));
        } else {
            return (E) deserializeVal(jedisOperator.srandmember(serializeKey(key)));
        }
    }

    /**
     * 判断 member 元素是否集合 key 的成员。
     */
    public boolean sismember(Object key, Object obj) {
        if (null == key || null == obj) {
            return false;
        }
        if (cluster) {
            return jedisCluster.sismember(serializeKey(key), serializeVal(obj));
        } else {
            return jedisOperator.sismember(serializeKey(key), serializeVal(obj));
        }
    }

    /**
     * 返回 key 中元素的数量
     */
    public Long scard(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.scard(serializeKey(key));
        } else {
            return jedisOperator.scard(serializeKey(key));
        }
    }

    //
    // sortedset
    // ---------------------------------------------------------------------------------------------------
    /**
     * 将一个或多个 member 元素及其 score 值加入到有序集 key 当中。
     * 如果某个 member 已经是有序集的成员，那么更新这个 member 的 score 值，并通过重新插入这个 member 元素，来保证该 member 在正确的位置上。
     * score 值可以是整数值或双精度浮点数。
     * 如果 key 不存在，则创建一个空的有序集并执行 ZADD 操作。
     * 当 key 存在但不是有序集类型时，返回一个错误。
     * 对有序集的更多介绍请参见 sorted set 。
     * 返回值:
     * 被成功添加的新成员的数量，不包括那些被更新的、已经存在的成员。
     */
    public Long zadd(Object key, Object obj, double score) {
        if (null == key || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.zadd(serializeKey(key), score, serializeVal(obj));
        } else {
            return jedisOperator.zadd(serializeKey(key), score, serializeVal(obj));
        }
    }

    /**
     * 移除有序集 key 中的一个或多个成员，不存在的成员将被忽略。
     * 当 key 存在但不是有序集类型时，返回一个错误。
     * 返回值:
     * 被成功移除的成员的数量，不包括被忽略的成员。
     */
    public Long zrem(Object key, Object obj) {
        if (null == key || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.zrem(serializeKey(key), serializeVal(obj));
        } else {
            return jedisOperator.zrem(serializeKey(key), serializeVal(obj));
        }
    }

    /**
     * 移除有序集 key 中，指定排名(rank)区间内的所有成员。
     * 区间分别以下标参数 start 和 stop 指出，包含 start 和 stop 在内。
     * 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
     * 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。
     * 返回值:
     * 被移除成员的数量。
     */
    public Long zremrangeByRank(Object key, int start, int end) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.zremrangeByRank(serializeKey(key), start, end);
        } else {
            return jedisOperator.zremrangeByRank(serializeKey(key), start, end);
        }
    }

    /**
     * 移除有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。
     * 返回值:
     * 被移除成员的数量。
     */
    public Long zremrangeByScore(Object key, double start, double end) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.zremrangeByScore(serializeKey(key), start, end);
        } else {
            return jedisOperator.zremrangeByScore(serializeKey(key), start, end);
        }
    }

    /**
     * 为有序集 key 的成员 member 的 score 值加上增量 increment 。
     * <p/>
     * 可以通过传递一个负数值 increment ，让 score 减去相应的值，比如 ZINCRBY key -5 member ，就是让 member 的 score 值减去 5 。 当 key 不存在，或 member 不是 key 的成员时， ZINCRBY key increment member 等同于 ZADD key increment member 。 当 key 不是有序集类型时，返回一个错误。 score 值可以是整数值或双精度浮点数。 返回值: member 成员的新 score 值，以字符串形式表示。
     */
    public Double zincrby(Object key, Object obj, double score) {
        if (null == key || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.zincrby(serializeKey(key), score, serializeVal(obj));
        } else {
            return jedisOperator.zincrby(serializeKey(key), score, serializeVal(obj));
        }
    }

    /**
     * 返回有序集 key 的基数。
     * 返回值:
     * 当 key 存在且是有序集类型时，返回有序集的基数。
     * 当 key 不存在时，返回 0 。
     */
    public Long zcard(Object key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.zcard(serializeKey(key));
        } else {
            return jedisOperator.zcard(serializeKey(key));
        }
    }

    /**
     * 返回有序集 key 中，成员 member 的 score 值。
     * 如果 member 元素不是有序集 key 的成员，或 key 不存在，返回 nil 。
     * 返回值:
     * member 成员的 score 值，以字符串形式表示。
     */
    public Double zscore(Object key, Object obj) {
        if (null == key || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.zscore(serializeKey(key), serializeVal(obj));
        } else {
            return jedisOperator.zscore(serializeKey(key), serializeVal(obj));
        }
    }

    /**
     * 返回有序集 key 中， score 值在 min 和 max 之间(默认包括 score 值等于 min 或 max )的成员的数量。
     * 返回值:
     * score 值在 min 和 max 之间的成员的数量。
     */
    public Long zcount(Object key, double min, double max) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return jedisCluster.zcount(serializeKey(key), min, max);
        } else {
            return jedisOperator.zcount(serializeKey(key), min, max);
        }
    }

    /**
     * 返回 key 中成员的排名。(从小到大)
     * <p/>
     * 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递增(从小到大)顺序排列。 排名以 0 为底，也就是说， score 值最小的成员排名为 0 。 使用 ZREVRANK 命令可以获得成员按 score 值递减(从大到小)排列的排名。 返回值: 如果 member 是有序集 key 的成员，返回 member 的排名。 如果 member 不是有序集 key 的成员，返回 nil 。
     */
    public Long zrank(Object key, Object obj) {
        if (null == key || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.zrank(serializeKey(key), serializeVal(obj));
        } else {
            return jedisOperator.zrank(serializeKey(key), serializeVal(obj));
        }
    }

    /**
     * 返回 key 中成员的排名。(从大到小)
     * <p/>
     * 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递减(从大到小)排序。 排名以 0 为底，也就是说， score 值最大的成员排名为 0 。 使用 ZRANK 命令可以获得成员按 score 值递增(从小到大)排列的排名。 返回值: 如果 member 是有序集 key 的成员，返回 member 的排名。 如果 member 不是有序集 key 的成员，返回 nil 。
     */
    public Long zrevrank(Object key, Object obj) {
        if (null == key || null == obj) {
            return null;
        }
        if (cluster) {
            return jedisCluster.zrevrank(serializeKey(key), serializeVal(obj));
        } else {
            return jedisOperator.zrevrank(serializeKey(key), serializeVal(obj));
        }
    }

    /**
     * 返回 key 中，指定区间内的成员。(从小到大)
     * <p/>
     * 其中成员的位置按 score 值递增(从小到大)来排序。 具有相同 score 值的成员按字典序(lexicographical order )来排列。 如果你需要成员按 score 值递减(从大到小)来排列，请使用 ZREVRANGE 命令。 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。 超出范围的下标并不会引起错误。 比如说，当 start 的值比有序集的最大下标还要大，或是 start > stop 时， ZRANGE 命令只是简单地返回一个空列表。 另一方面，假如 stop 参数的值比有序集的最大下标还要大，那么 Redis 将 stop 当作最大下标来处理。 可以通过使用 WITHSCORES 选项，来让成员和它的 score 值一并返回，返回列表以 value1,score1, ..., valueN,scoreN 的格式表示。 客户端库可能会返回一些更复杂的数据类型，比如数组、元组等。 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     */
    public <E> Set<E> zrange(Object key, int start, int end) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertBytesCollectionToSet_deserializeVal(jedisCluster.zrange(serializeKey(key), start, end));
        } else {
            return convertBytesCollectionToSet_deserializeVal(jedisOperator.zrange(serializeKey(key), start, end));
        }

    }

    /**
     * 返回有序集 key 中，指定区间内的成员。(从大到小)
     * <p/>
     * 其中成员的位置按 score 值递减(从大到小)来排列。 具有相同 score 值的成员按字典序的逆序(reverse lexicographical order)排列。 除了成员按 score 值递减的次序排列这一点外， ZREVRANGE 命令的其他方面和 ZRANGE 命令一样。 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     */
    public <E> Set<E> zrevrange(Object key, int start, int end) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertBytesCollectionToSet_deserializeVal(jedisCluster.zrevrange(serializeKey(key), start, end));
        } else {
            return convertBytesCollectionToSet_deserializeVal(jedisOperator.zrevrange(serializeKey(key), start, end));
        }
    }

    /**
     * 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。按 score 值递增(从小到大)次序排列
     * 具有相同 score 值的成员按字典序(lexicographical order)来排列(该属性是有序集提供的，不需要额外的计算)。
     * 可选的 LIMIT 参数指定返回结果的数量及区间(就像SQL中的 SELECT LIMIT offset, count )，
     * 注意当 offset 很大时，定位 offset 的操作可能需要遍历整个有序集，此过程最坏复杂度为 O(N) 时间。
     * 可选的 WITHSCORES 参数决定结果集是单单返回有序集的成员，还是将有序集成员及其 score 值一起返回。
     * 返回值:
     * 指定区间内，带有 score 值(可选)的有序集成员的列表。
     */
    public <E> Set<E> zrangeByScore(Object key, double min, double max) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertBytesCollectionToSet_deserializeVal(jedisCluster.zrangeByScore(serializeKey(key), min, max));
        } else {
            return convertBytesCollectionToSet_deserializeVal(jedisOperator.zrangeByScore(serializeKey(key), min, max));
        }
    }

    /**
     * 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。按 score 值递增(从小到大)次序排列。
     * 具有相同 score 值的成员按字典序(lexicographical order)来排列(该属性是有序集提供的，不需要额外的计算)。
     * 可选的 LIMIT 参数指定返回结果的数量及区间(就像SQL中的 SELECT LIMIT offset, count )，
     * 注意当 offset 很大时，定位 offset 的操作可能需要遍历整个有序集，此过程最坏复杂度为 O(N) 时间。
     * 可选的 WITHSCORES 参数决定结果集是单单返回有序集的成员，还是将有序集成员及其 score 值一起返回。
     * 返回值:
     * 指定区间内，带有 score 值(可选)的有序集成员的列表。
     */
    public <E> Set<E> zrangeByScore(Object key, double min, double max, int offset, int count) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertBytesCollectionToSet_deserializeVal(jedisCluster.zrangeByScore(serializeKey(key), min, max, offset, count));
        } else {
            return convertBytesCollectionToSet_deserializeVal(jedisOperator.zrangeByScore(serializeKey(key), min, max, offset, count));
        }
    }

    /**
     * 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。按 score 值递增(从小到大)次序排列。(包含score)
     */
    public <E> Set<E> zrevrangeByScore(Object key, double min, double max) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertBytesCollectionToSet_deserializeVal(jedisCluster.zrevrangeByScore(serializeKey(key), min, max));
        } else {
            return convertBytesCollectionToSet_deserializeVal(jedisOperator.zrevrangeByScore(serializeKey(key), min, max));
        }
    }

    /**
     * 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。按 score 值递增(从小到大)次序排列。(包含score)
     */
    public <E> Set<E> zrevrangeByScore(Object key, double min, double max, int offset, int count) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertBytesCollectionToSet_deserializeVal(jedisCluster.zrevrangeByScore(serializeKey(key), min, max, offset, count));
        } else {
            return convertBytesCollectionToSet_deserializeVal(jedisOperator.zrevrangeByScore(serializeKey(key), min, max, offset, count));
        }
    }

    /**
     * 返回 key 中，指定区间内的成员。(从小到大) (包含score)
     */
    public Set<Tuple> zrangeWithScores(Object key, int start, int end) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertTupleToTupl(jedisCluster.zrangeWithScores(serializeKey(key), start, end));
        } else {
            return convertTupleToTupl(jedisOperator.zrangeWithScores(serializeKey(key), start, end));
        }
    }

    /**
     * 返回有序集 key 中，指定区间内的成员。(从大到小) (包含score)
     */
    public Set<Tuple> zrevrangeWithScores(Object key, int start, int end) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertTupleToTupl(jedisCluster.zrevrangeWithScores(serializeKey(key), start, end));
        } else {
            return convertTupleToTupl(jedisOperator.zrevrangeWithScores(serializeKey(key), start, end));
        }
    }

    /**
     * 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。按 score 值递增(从小到大)次序排列 (包含score)
     */
    public Set<Tuple> zrangeByScoreWithScores(Object key, double min, double max) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertTupleToTupl(jedisCluster.zrangeByScoreWithScores(serializeKey(key), min, max));
        } else {
            return convertTupleToTupl(jedisOperator.zrangeByScoreWithScores(serializeKey(key), min, max));
        }
    }

    /**
     * 返回有序集 key 中，指定区间内的成员。(从大到小) (包含score)
     */
    public Set<Tuple> zrevrangeByScoreWithScores(Object key, double max, double min) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertTupleToTupl(jedisCluster.zrevrangeByScoreWithScores(serializeKey(key), max, min));
        } else {
            return convertTupleToTupl(jedisOperator.zrevrangeByScoreWithScores(serializeKey(key), max, min));
        }
    }

    /**
     * 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。按 score 值递增(从小到大)次序排列。(包含score)
     */
    public Set<Tuple> zrangeByScoreWithScores(Object key, double min, double max, int offset, int count) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertTupleToTupl(jedisCluster.zrangeByScoreWithScores(serializeKey(key), min, max, offset, count));
        } else {
            return convertTupleToTupl(jedisOperator.zrangeByScoreWithScores(serializeKey(key), min, max, offset, count));
        }
    }

    /**
     * 返回有序集 key 中，指定区间内的成员。(从大到小) (包含score)
     */
    public Set<Tuple> zrevrangeByScoreWithScores(Object key, double max, double min, int offset, int count) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertTupleToTupl(jedisCluster.zrevrangeByScoreWithScores(serializeKey(key), max, min, offset, count));
        } else {
            return convertTupleToTupl(jedisOperator.zrevrangeByScoreWithScores(serializeKey(key), max, min, offset, count));
        }
    }

    //
    // pubsub
    // ---------------------------------------------------------------------------------------------------
    public void subscribe(final JedisPubSub jedisPubSub, final String... channels) {
        if (cluster) {
            jedisCluster.subscribe(jedisPubSub, channels);
        } else {
            jedisOperator.subscribe(jedisPubSub, channels);
        }
    }

    public void subscribe(final BinaryJedisPubSub jedisPubSub, final String... channels) {
        if (cluster) {
            jedisCluster.subscribe(jedisPubSub, convertObjectArrayToByteArray_serializeKey(channels));
        } else {
            jedisOperator.subscribe(jedisPubSub, convertObjectArrayToByteArray_serializeKey(channels));
        }
    }

    public Long publish(final String channel, final String message) {
        if (cluster) {
            return jedisCluster.publish(channel, message);
        } else {
            return jedisOperator.publish(channel, message);
        }
    }

    public Long publish(final String channel, final Object message) {
        if (cluster) {
            return jedisCluster.publish(serializeKey(channel), serializeVal(message));
        } else {
            return jedisOperator.publish(serializeKey(channel), serializeVal(message));
        }
    }

    public Long publish(final String channel, final byte[] message) {
        if (cluster) {
            return jedisCluster.publish(serializeKey(channel), message);
        } else {
            return jedisOperator.publish(serializeKey(channel), message);
        }
    }

    //
    // all
    // ---------------------------------------------------------------------------------------------------

    /**
     * 返回或保存给定列表、集合、有序集合 key 中经过排序的元素。
     * 排序默认以数字作为对象，值被解释为双精度浮点数，然后进行比较。
     */
    public <E> List<E> sort(String key) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertBytesCollectionToList_deserializeVal(jedisCluster.sort(serializeKey(key)));
        } else {
            return convertBytesCollectionToList_deserializeVal(jedisOperator.sort(serializeKey(key)));
        }
    }

    /**
     * 返回或保存给定列表、集合、有序集合 key 中经过排序的元素。
     * 排序默认以数字作为对象，值被解释为双精度浮点数，然后进行比较。
     */
    public <E> List<E> sort(Object key, SortingParams sortingParameters) {
        if (null == key) {
            return null;
        }
        if (cluster) {
            return convertBytesCollectionToList_deserializeVal(jedisCluster.sort(serializeKey(key), sortingParameters));
        } else {
            return convertBytesCollectionToList_deserializeVal(jedisOperator.sort(serializeKey(key), sortingParameters));
        }
    }

    // ---------------------------------------------------------------------------------------------------
    public Set<Tuple> convertTupleToTupl(final Set<redis.clients.jedis.Tuple> tuples) {
        Set<Tuple> tupls = Sets.newHashSet();
        for (redis.clients.jedis.Tuple tuple : tuples) {
            Object element = deserializeVal(tuple.getBinaryElement());
            Double score = tuple.getScore();
            tupls.add(new Tuple(element, score));
        }
        return tupls;
    }

    public static byte[][] convertByteListToByteArray(List<byte[]> args) {
        return args.toArray(new byte[args.size()][0]);
    }

    public <T> byte[][] convertObjectArrayToByteArray_serializeKey(final T[] array) {
        if (null == array || array.length == 0) {
            return new byte[0][0];
        }
        int len = array.length;
        List<byte[]> list = new ArrayList<byte[]>(len);
        for (int i = 0; i < len; i++) {
            list.add(serializeKey(String.valueOf(array[i])));
        }
        return list.toArray(new byte[len][0]);
    }

    @SuppressWarnings("unchecked")
    public <E> List<E> convertBytesCollectionToList_deserializeVal(Collection<byte[]> bytes) {
        List<E> data = Lists.newArrayList();
        for (byte[] bs : bytes) {
            data.add((E) deserializeVal(bs));
        }
        return data;
    }

    @SuppressWarnings("unchecked")
    public <E> Set<E> convertBytesCollectionToSet_deserializeVal(Collection<byte[]> bytes) {
        Set<E> data = Sets.newHashSet();
        for (byte[] bs : bytes) {
            data.add((E) deserializeVal(bs));
        }
        return data;
    }

    //
    // Serialize
    // ---------------------------------------------------------------------------------------------------
    public byte[] serializeKey(Object key) {
        return keySerializer.serialize(key);
    }

    public String deserializeKey(byte[] obj) {
        return (String) keySerializer.deserialize(obj);
    }

    public byte[] serializeVal(final Object obj) {
        return valSerializer.serialize(obj);
    }

    @SuppressWarnings("unchecked")
    public <T> T deserializeVal(final byte[] bytes) {
        return (T) valSerializer.deserialize(bytes);
    }

    //
    // getter & setter
    // ---------------------------------------------------------------------------------------------------

    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }

    public void setJedisCluster(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public JedisOperator getJedisOperator() {
        return jedisOperator;
    }

    public void setJedisOperator(JedisOperator jedisOperator) {
        this.jedisOperator = jedisOperator;
    }

    public Serializer getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(Serializer keySerializer) {
        this.keySerializer = keySerializer;
    }

    public Serializer getValSerializer() {
        return valSerializer;
    }

    public void setValSerializer(Serializer valSerializer) {
        this.valSerializer = valSerializer;
    }
}