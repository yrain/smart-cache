package com.smart.cache;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.smart.cache.Cache.Level;
import com.smart.cache.Cache.Operator;
import com.smart.jedis.JedisTemplate;
import com.smart.util.Dates;
import com.smart.util.Objects;
import com.smart.util.Utils;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.DiskStoreConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

/**
 * CacheTemplate
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class CacheTemplate implements InitializingBean {

    public static final Logger                               logger                               = LoggerFactory.getLogger(CacheTemplate.class);

    //
    // 配置项
    // ---------------------------------------------------------------------------------------------------------------------------
    private String                                           key                                  = "smart";
    private String                                           spliter                              = ":";
    // 是否启用本地缓存
    private boolean                                          localEnabled                         = true;
    // 是否开启set同步命令
    private boolean                                          setCmdEnabled                        = false;

    // ehcache设置
    // ---------------------------------------------------------------------------------------------------------------------------
    // 本地缓存存储磁盘位置
    private String                                           localStoreLocation                   = "/cache/";
    // 本地缓存最大内存大小
    private String                                           localMaxBytesLocalHeap               = "256M";
    // 本地缓存最大磁盘大小
    private String                                           localMaxBytesLocalDisk               = "1024M";
    // 本地缓存15分钟过期
    private int                                              localTimeToIdleSeconds               = 15 * 60;
    // 本地缓存ttl默认值,为使本地缓存和远程缓存TTL一致.故设置为0
    private final int                                        localTimeToLiveSeconds               = 0;
    // 本地缓存3分钟清理一次
    private int                                              localDiskExpiryThreadIntervalSeconds = 3 * 60;
    // fetch命令最长等待5秒
    private int                                              fetchTimeoutSeconds                  = 5;

    private JedisTemplate                                    jedisTemplate;
    private net.sf.ehcache.CacheManager                      cacheManager;
    private CacheSync                                        cacheSync;
    private final ConcurrentHashMap<String, Future<Ehcache>> ehcaches                             = new ConcurrentHashMap<>();

    @SuppressWarnings("deprecation")
    @Override
    public void afterPropertiesSet() throws Exception {
        Cache.ID = key + "." + Dates.newDateStringOfFormatDateTimeSSSNoneSpace();
        Cache.HOST = Utils.getLocalHostIP();
        Cache.CACHE_STORE = key + spliter + "cache" + spliter + "store";
        Cache.CACHE_STORE_SYNC = Cache.CACHE_STORE + spliter + "sync";
        if (this.localEnabled) {
            Configuration configuration = new Configuration();
            configuration.setName(Cache.ID);
            configuration.setMaxBytesLocalHeap(localMaxBytesLocalHeap);
            configuration.setMaxBytesLocalDisk(localMaxBytesLocalDisk);
            // DiskStore
            // 每次启动设置新的文件地址,以避免重启期间一级缓存未同步,以及单机多应用启动造成EhcacheManager重复的问题.
            DiskStoreConfiguration dsc = new DiskStoreConfiguration();
            dsc.setPath(localStoreLocation + Cache.ID);
            configuration.diskStore(dsc);
            // DefaultCache
            CacheConfiguration defaultCacheConfiguration = new CacheConfiguration();
            defaultCacheConfiguration.setEternal(false);
            defaultCacheConfiguration.setTimeToIdleSeconds(localTimeToIdleSeconds);
            defaultCacheConfiguration.setTimeToLiveSeconds(localTimeToLiveSeconds);
            defaultCacheConfiguration.setOverflowToDisk(true);
            defaultCacheConfiguration.setDiskPersistent(false);
            defaultCacheConfiguration.memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU);
            defaultCacheConfiguration.setDiskExpiryThreadIntervalSeconds(localDiskExpiryThreadIntervalSeconds);
            // 默认false,使用引用.设置为true,避免外部代码修改了缓存对象.造成EhCache的缓存对象也随之改变
            defaultCacheConfiguration.copyOnRead(true);
            defaultCacheConfiguration.copyOnWrite(true);
            configuration.setDefaultCacheConfiguration(defaultCacheConfiguration);
            configuration.setDynamicConfig(false);
            configuration.setUpdateCheck(false);
            this.cacheManager = new CacheManager(configuration);
            this.cacheSync = new RedisPubSubSync(this);// 使用Redis Topic发送订阅缓存变更消息
        }
    }

    //
    // set
    // ---------------------------------------------------------------------------------------------------
    /**
     * 设置缓存
     */
    public void set(String name, String key, Object value) {
        this.set(name, key, value, Level.Remote);
        if (localEnabled) {
            this.set(name, key, value, Level.Local);
            if (this.setCmdEnabled) {
                this.sendSetCmd(name, key);
            } else {
                this.sendDelCmd(name, key);
            }
        }
    }

    /**
     * 设置缓存(根据缓存级别)
     */
    protected void set(String name, String key, Object value, Level level) {
        if (level.equals(Level.Local)) {
            if (!localEnabled) {
                return;
            }
            this.getEhcache(name).put(new Element(key, value, false, localTimeToIdleSeconds, localTimeToLiveSeconds));
        } else {
            this.syncToRedis(name, key, value, Operator.SET);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("set > name:" + name + ",key:" + key + ",tti:" + this.tti(name, key) + ",ttl:" + this.ttl(name, key, level) + ",level:" + level);
        }
    }

    //
    // set with ttl
    // ---------------------------------------------------------------------------------------------------
    /**
     * 设置缓存与过期时间
     */
    public void set(String name, String key, Object value, int ttl) {   // 先设置远程缓存成功后,再设置本地缓存
        this.set(name, key, value, ttl, Level.Remote);
        if (localEnabled) {
            this.set(name, key, value, ttl, Level.Local);
            if (this.setCmdEnabled) {
                this.sendSetCmd(name, key);
            } else {
                this.sendDelCmd(name, key);
            }
        }
    }

    /**
     * 设置缓存与过期时间(根据缓存级别)
     */
    protected void set(String name, String key, Object value, int ttl, Level level) {
        if (level.equals(Level.Local)) {
            if (!localEnabled) {
                return;
            }
            if (ttl <= 0) {
                // 当远程缓存ttl=0永久有效时,则使用本地缓存默认的tti和ttl
                this.getEhcache(name).put(new Element(key, value, false, localTimeToIdleSeconds, ttl));
            } else {
                // 当远程缓存非永久有效时,若远程缓存的ttl小于本地缓存默认的ttl,则使用远程缓存的ttl,反之亦然.
                int tti = ttl < localTimeToIdleSeconds ? ttl : localTimeToIdleSeconds;
                this.getEhcache(name).put(new Element(key, value, false, tti, ttl));
            }
        } else {
            this.syncToRedis(name, key, value, ttl, Operator.SET); // 记录缓存名称到Redis
        }
        if (logger.isDebugEnabled()) {
            logger.debug("set > name:" + name + ",key:" + key + ",tti:" + this.tti(name, key) + ",ttl:" + this.ttl(name, key, level) + ",level:" + level);
        }
    }

    //
    // setIfAbsent
    // ---------------------------------------------------------------------------------------------------
    /**
     * 若缓存不存在则设置,若存在则返回原缓存值
     */
    public <T> T setIfAbsent(String name, String key, Object value) {
        T existing = this.get(name, key);
        if (existing == null) {
            return existing;
        } else {
            this.set(name, key, value);
            return null;
        }
    }

    //
    // setIfAbsent with ttl
    // ---------------------------------------------------------------------------------------------------
    /**
     * 若缓存不存在则设置缓存与过期时间,若存在则返回原缓存值
     */
    public <T> T setIfAbsent(String name, String key, Object value, int ttl) {
        T existing = this.get(name, key);
        if (existing == null) {
            return existing;
        } else {
            this.set(name, key, value, ttl);
            return null;
        }
    }

    //
    // get
    // ---------------------------------------------------------------------------------------------------
    /**
     * 获取缓存值
     */
    public <T> T get(String name, String key) {
        T value = null;
        if (localEnabled) {
            value = this.get(name, key, Level.Local);
        }
        if (value == null) {
            value = this.get(name, key, Level.Remote);
        }
        return value;
    }

    /**
     * 获取缓存值(根据缓存级别)
     */
    @SuppressWarnings("unchecked")
    protected <T> T get(String name, String key, Level level) {
        T value = null;
        if (level.equals(Level.Local)) {
            if (!localEnabled) {
                return null;
            }
            if (!this.ehcaches.containsKey(name)) {
                return null;
            }
            Element element = this.getEhcache(name).get(key);
            if (logger.isDebugEnabled()) {
                logger.debug("get > name:" + name + ",key:" + key + ",tti:" + this.tti(name, key) + ",ttl:" + this.ttl(name, key, level) + ",level:" + level);
            }
            if (element != null) {
                value = (T) element.getObjectValue();
                // 因设置CopyOnRead:true,在读取中不会更新Element的tti,所以需要手动刷新
                int ttl = this.ttl(name, key, Level.Local);
                // this.set(name, key, value, ttl, Level.Local); // 避免打印日志.屏蔽,且使用如下重复代码刷新
                if (ttl <= 0) {
                    // 当远程缓存ttl=0永久有效时,则使用本地缓存默认的tti和ttl
                    this.getEhcache(name).put(new Element(key, value, false, localTimeToIdleSeconds, ttl));
                } else {
                    // 当远程缓存非永久有效时,若远程缓存的ttl小于本地缓存默认的ttl,则使用远程缓存的ttl,反之亦然.
                    int tti = ttl < localTimeToIdleSeconds ? ttl : localTimeToIdleSeconds;
                    this.getEhcache(name).put(new Element(key, value, false, tti, ttl));
                }
            }
        } else {
            value = this.jedisTemplate.get(this.getRedisKeyOfElement(name, key));
            if (logger.isDebugEnabled()) {
                logger.debug("get > name:" + name + ",key:" + key + ",tti:" + this.tti(name, key) + ",ttl:" + this.ttl(name, key, level) + ",level:" + level);
            }
            if (value != null) {
                int ttl = this.ttl(name, key, Level.Remote);
                if (ttl < 0) {
                    // key 已经失效
                    // ignore...
                } else {
                    this.set(name, key, value, ttl, Level.Local);
                }
            }
        }
        return value;
    }

    //
    // del
    // ---------------------------------------------------------------------------------------------------
    /**
     * 删除单个缓存值
     */
    public void del(String name, String key) {
        this.sendDelCmd(name, key);
        this.del(name, key, Level.Local);
        this.del(name, key, Level.Remote);
    }

    /**
     * 删除单个缓存值(根据缓存级别)
     */
    protected void del(String name, String key, Level level) {
        if (level.equals(Level.Local)) {
            if (!localEnabled) {
                return;
            }
            if (this.ehcaches.containsKey(name)) {
                this.getEhcache(name).remove(key);
            }
        } else {
            this.syncToRedis(name, key, Operator.DEL);
        }
        logger.debug("del > name:" + name + ",key:" + key + ",level:" + level);
    }

    //
    // rem
    // ---------------------------------------------------------------------------------------------------
    /**
     * 删除指定name下所有缓存
     */
    public void rem(String name) {
        this.sendRemCmd(name);
        this.rem(name, Level.Local);
        this.rem(name, Level.Remote);
    }

    /**
     * 删除指定name下所有缓存(根据缓存级别)
     */
    protected void rem(String name, Level level) {
        if (level.equals(Level.Local)) {
            if (!localEnabled) {
                return;
            }
            if (this.ehcaches.containsKey(name)) {
                this.getEhcache(name).removeAll();
                this.ehcaches.remove(name);
            }
        } else {
            this.syncToRedis(name, Operator.REM);
        }
        logger.debug("rem > name:" + name + ",level:" + level);
    }

    //
    // cls
    // ---------------------------------------------------------------------------------------------------
    /**
     * 清除所有缓存
     */
    public void cls() {
        this.sendClsCmd();
        this.cls(Level.Local);
        this.cls(Level.Remote);
    }

    /**
     * 清除所有缓存(根据缓存级别)
     */
    protected void cls(Level level) {
        if (level.equals(Level.Local)) {
            if (!localEnabled) {
                return;
            }
            this.cacheManager.clearAll();
            this.ehcaches.clear();
        } else {
            syncToRedis(Operator.CLS);
        }
        logger.debug("cls > " + "level:" + level);
    }

    //
    // ttl
    // ---------------------------------------------------------------------------------------------------
    /**
     * 获取缓存过期剩余时间.单位为秒
     * 0,永久
     * -1,不存在
     */
    public int ttl(String name, String key) {
        int ttl = -1;
        if (localEnabled) {
            ttl = this.ttl(name, key, Level.Local);
        }
        if (ttl <= -1) {
            ttl = this.ttl(name, key, Level.Remote);
        }
        return ttl;
    }

    /**
     * 获取缓存过期剩余时间.单位为秒
     * 0,永久
     * -1,不存在
     */
    public int ttl(String name, String key, Level level) {
        int ttl = -1;
        if (level.equals(Level.Local)) {
            if (!localEnabled) {
                return -1;
            }
            if (this.ehcaches.containsKey(name)) {
                Element element = this.getEhcache(name).get(key);
                if (element != null) {
                    ttl = element.getTimeToLive();
                    if (ttl != 0) {
                        ttl = ttl - (int) ((System.currentTimeMillis() - element.getCreationTime()) / 1000);
                    }
                }
            }
        } else {
            ttl = this.jedisTemplate.ttl(this.getRedisKeyOfElement(name, key)).intValue();
        }
        return ttl;
    }

    /**
     * 获取本地缓存idle时间.单位为秒
     */
    public int tti(String name, String key) {
        int ttl = -1;
        if (!localEnabled) {
            return -1;
        }
        if (this.ehcaches.containsKey(name)) {
            Element element = this.getEhcache(name).get(key);
            if (element != null) {
                ttl = element.getTimeToIdle();
                if (ttl != 0) {
                    ttl = ttl - (int) ((System.currentTimeMillis() - element.getCreationTime()) / 1000);
                }
            }
        }
        return ttl;
    }

    //
    // exists
    // ---------------------------------------------------------------------------------------------------
    /**
     * 判断缓存是否存在,以远程缓存为准.
     */
    public boolean isExists(String name, String key) {
        return this.isExists(name, key, Level.Remote);
    }

    /**
     * 判断缓存是否存在,以远程缓存为准.
     */
    public boolean isExists(String name, String key, Level level) {
        boolean flag = false;
        if (level.equals(Level.Local)) {
            if (this.ehcaches.containsKey(name)) {
                Element element = this.getEhcache(name).get(key);
                flag = element != null;
            }
        } else {
            flag = this.jedisTemplate.exists(this.getRedisKeyOfElement(name, key));
        }
        if (logger.isDebugEnabled()) {
            logger.debug("exists > name:" + name + ",key:" + key + ",level:" + level + ",exists:" + flag);
        }
        return flag;
    }

    //
    // fetch
    // ---------------------------------------------------------------------------------------------------
    /**
     * 抓取多机一级缓存数据
     */
    public List<CacheData> fetch(String name, String key) {
        logger.debug("fetch > name:" + name + ",key:" + key);
        long waitMaxTime = System.currentTimeMillis() + this.fetchTimeoutSeconds * 1000;
        String fetch = this.getRedisKeyOfElement(name, key) + spliter + "fetch" + spliter + Dates.newDateStringOfFormatDateTimeSSSNoneSpace();
        this.sendFetchCmd(name, key, fetch);
        do {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignore...
            }
            boolean isWrited = this.jedisTemplate.exists(fetch);
            if (isWrited) {
                List<CacheData> datas = this.getJedisTemplate().hvals(fetch);
                for (int i = 0; i < datas.size(); i++) {
                    CacheData data = datas.get(i);
                    if (data.getTtl() > -1) {
                        return datas;
                    }
                }
            }
        } while (System.currentTimeMillis() < waitMaxTime);
        logger.debug("fetch > name:" + name + ",key:" + key + ",timeout:" + this.fetchTimeoutSeconds);
        return this.getJedisTemplate().hvals(fetch);
    }

    public CacheData getCacheData(String name, String key, Level level) {
        String value = Objects.toString(this.get(name, key, level));
        int tti = -1;
        if (Level.Local.equals(level)) {
            tti = this.tti(name, key);
        }
        int ttl = this.ttl(name, key, level);
        return new CacheData(name, key, value, tti, ttl, level);
    }

    //
    // public
    // ---------------------------------------------------------------------------------------------------
    /**
     * 获取所有缓存名称
     */
    public Set<String> names() {
        return this.getCaches();
    }

    /**
     * 获取name下所有缓存Key
     */
    public Set<String> keys(String name) {
        return this.getElements(name);
    }

    /**
     * 获取name下所有缓存值
     */
    public <E> List<E> values(String name) {
        try {
            Set<String> elements = this.getElements(name);
            if (null != elements && !elements.isEmpty()) {
                List<String> keys = Lists.newArrayList();
                for (String field : elements) {
                    keys.add(this.getRedisKeyOfElement(name, field));
                }
                return this.jedisTemplate.mget(keys);
            } else {
                return Collections.emptyList();
            }
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    /**
     * 获取name下所有缓存数量
     */
    public int size(String name) {
        return this.getElements(name).size();
    }

    /**
     * 获取name下所有缓存名称,以及其缓存Key
     */
    public Map<String, Set<String>> stores() {
        Map<String, Set<String>> cachemaps = Maps.newHashMap();
        Set<String> caches = this.getCaches();
        for (String cache : caches) {
            cachemaps.put(cache, this.getElements(cache));
        }
        return cachemaps;
    }

    /**
     * 关闭
     */
    public void shutdown() {
        this.cacheManager.shutdown();
    }

    //
    // SyncToRedis
    // ---------------------------------------------------------------------------------------------------
    /**
     * 以如下数据格式同步单机缓存至全局Redis中保存.实现统一查询,抓取,删除等功能.
     * (类似Ehcache:Cache.Element.Value)
     * smart:cache:store
     * - user
     * smart:cache:store:user
     * - John
     * - Terry
     * smart:cache:store:user:John
     * smart:cache:store:user:Terry
     */
    private void syncToRedis(String name, String field, Object value, int timeToLiveSeconds, Operator operator) {
        if (operator.equals(Operator.SET)) {
            int ttl = 0;
            ttl = Optional.fromNullable(timeToLiveSeconds).or(ttl);
            // 存入
            // smart:cache:store:user:John
            // smart:cache:store:user:Terry
            if (ttl == 0) {
                this.jedisTemplate.set(this.getRedisKeyOfElement(name, field), value);
            } else {
                this.jedisTemplate.set(this.getRedisKeyOfElement(name, field), value, ttl);
            }
            // 创建数据结构
            // 存入
            // smart:cache:store
            // - user
            this.jedisTemplate.sadd(this.getRedisKeyOfStore(), name);
            // 存入
            // smart:cache:store:user
            // - John
            // - Terry
            this.jedisTemplate.hset(this.getRedisKeyOfCache(name), field, timeToLiveSeconds);
        }
        // Operator.DEL
        else if (operator.equals(Operator.DEL)) {
            this.jedisTemplate.del(this.getRedisKeyOfElement(name, field)); // 删除 smart:cache:store:user:John
            this.jedisTemplate.hdel(this.getRedisKeyOfCache(name), field); // 删除 smart:cache:store:user.John
        }
        // Operator.REM
        else if (operator.equals(Operator.REM)) {
            Set<String> storefields = this.jedisTemplate.hkeys(this.getRedisKeyOfCache(name)); // 获取 smart:cache:store:user 所有 field
            List<String> deletekeys = Lists.newArrayList();
            for (String storefield : storefields) {
                deletekeys.add(this.getRedisKeyOfElement(name, storefield));// 依次取Key,并记录到要删除key列表
            }
            deletekeys.add(this.getRedisKeyOfCache(name)); // 将 smart:cache:store:user,记录到要删除key列表
            this.jedisTemplate.mdel(deletekeys);// 批量删除
            this.jedisTemplate.srem(this.getRedisKeyOfStore(), name);// 删除 smart:cache:store:user
        }
        // Operator.CLS
        else if (operator.equals(Operator.CLS)) {
            Set<String> caches = this.getCaches();  // 获取 smart:cache:store 里所有 cache
            List<String> deletekeys = Lists.newArrayList();
            for (String cache : caches) {
                Set<String> storefields = this.getElementsWithOutExpireCheck(cache); // 获取 smart:cache:store:user 所有 field
                for (String storefield : storefields) {
                    deletekeys.add(this.getRedisKeyOfElement(cache, storefield));// 依次取Key,并记录到要删除key列表
                }
                deletekeys.add(this.getRedisKeyOfCache(cache)); // 将smart:cache:store:user,记录到要删除key列表
            }
            deletekeys.add(this.getRedisKeyOfStore());
            this.jedisTemplate.mdel(deletekeys);
        }
        //
        else {
            logger.warn("SyncToRedis > Unknown Operator");
        }
    }

    private void syncToRedis(Operator operator) {
        this.syncToRedis(null, null, null, 0, operator);
    }

    private void syncToRedis(String name, Operator operator) {
        this.syncToRedis(name, null, null, 0, operator);
    }

    private void syncToRedis(String name, String field, Operator operator) {
        this.syncToRedis(name, field, null, 0, operator);
    }

    private void syncToRedis(String name, String field, Object value, Operator operator) {
        this.syncToRedis(name, field, value, 0, operator);
    }

    //
    // send
    // ---------------------------------------------------------------------------------------------------
    /**
     * 发送新增缓存命令
     */
    private void sendSetCmd(String name, String key) {
        if (localEnabled) {
            Command c = Command.set(name, key);
            cacheSync.sendCommand(c);
            logger.debug("sendSetCmd > " + "name:" + name + ",key:" + key);
        }
    }

    /**
     * 发送删除缓存命令
     */
    private void sendDelCmd(String name, String key) {
        if (localEnabled) {
            Command c = Command.del(name, key);
            cacheSync.sendCommand(c);
            logger.debug("sendDelCmd > " + "name:" + name + ",key:" + key);
        }
    }

    /**
     * 发送移除缓存命令
     */
    private void sendRemCmd(String name) {
        if (localEnabled) {
            Command c = Command.rem(name);
            cacheSync.sendCommand(c);
            logger.debug("sendRemCmd > " + "name:" + name);
        }
    }

    /**
     * 发送多机获取本地缓存命令
     */
    private void sendFetchCmd(String name, String key, String fetch) {
        if (localEnabled) {
            Command c = Command.fetch(name, key, fetch);
            cacheSync.sendCommand(c);
            logger.debug("sendFetchCmd > " + "name:" + name + ",key:" + key);
        }
    }

    /**
     * 发送清空缓存命令
     */
    private void sendClsCmd() {
        if (localEnabled) {
            Command c = Command.cls();
            cacheSync.sendCommand(c);
            logger.debug("sendClsCmd");
        }
    }

    //
    // private
    // ---------------------------------------------------------------------------------------------------
    /**
     * 创建本地缓存
     */
    private Ehcache getEhcache(final String name) {
        Future<Ehcache> future = this.ehcaches.get(name);
        if (future == null) {
            Callable<Ehcache> callable = new Callable<Ehcache>() {
                @Override
                public Ehcache call() throws Exception {
                    Ehcache cache = cacheManager.getEhcache(name);
                    if (cache == null) {
                        cacheManager.addCache(name);
                        cache = cacheManager.getEhcache(name);
                    }
                    return cache;
                }
            };
            FutureTask<Ehcache> task = new FutureTask<>(callable);
            future = this.ehcaches.putIfAbsent(name, task);
            if (future == null) {
                future = task;
                task.run();
            }
        }
        try {
            return future.get();
        } catch (Exception e) {
            this.ehcaches.remove(name);
            throw new CacheException(e);
        }
    }

    private Set<String> getCaches() {
        Set<String> caches = Sets.newHashSet();
        Set<String> memebers = this.jedisTemplate.smembers(this.getRedisKeyOfStore());
        List<String> deletes = Lists.newArrayList();
        for (String memeber : memebers) {
            int ttl = this.jedisTemplate.ttl(this.getRedisKeyOfCache(memeber)).intValue();
            if (ttl < 0) {
                deletes.add(memeber);
                this.jedisTemplate.srem(this.getRedisKeyOfStore(), memeber);
                continue;
            }
            caches.add(memeber);
        }
        return caches;
    }

    private Set<String> getElements(String cache) {
        Set<String> elements = Sets.newHashSet();
        Map<String, Integer> cachefields = this.jedisTemplate.hgetAll(this.getRedisKeyOfCache(cache));
        List<String> deletes = Lists.newArrayList();
        for (String field : cachefields.keySet()) {
            Integer data = cachefields.get(field);
            if (data > 0) {
                int ttl = this.jedisTemplate.ttl(this.getRedisKeyOfElement(cache, field)).intValue();
                if (ttl < 0) {
                    // 若已失效
                    this.del(cache, field);
                    deletes.add(this.getRedisKeyOfCache(cache));
                    continue;
                }
            }
            elements.add(field);
        }
        this.jedisTemplate.mdel(deletes);
        return elements;
    }

    private Set<String> getElementsWithOutExpireCheck(String cache) {
        return this.jedisTemplate.hkeys(this.getRedisKeyOfCache(cache));
    }

    //
    // private getRedisKeyOf
    // ---------------------------------------------------------------------------------------------------
    private String getRedisKeyOfStore() {
        return Cache.CACHE_STORE;
    }

    private String getRedisKeyOfCache(String name) {
        return Cache.CACHE_STORE + spliter + name;
    }

    private String getRedisKeyOfElement(String name, String key) {
        return Cache.CACHE_STORE + spliter + name + spliter + key;
    }

    //
    // getter & setter
    // ---------------------------------------------------------------------------------------------------
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public JedisTemplate getJedisTemplate() {
        return jedisTemplate;
    }

    public void setJedisTemplate(JedisTemplate jedisTemplate) {
        this.jedisTemplate = jedisTemplate;
    }

    public CacheSync getCacheSync() {
        return cacheSync;
    }

    public void setCacheSync(CacheSync cacheSync) {
        this.cacheSync = cacheSync;
    }

    public String getSpliter() {
        return spliter;
    }

    public void setSpliter(String spliter) {
        this.spliter = spliter;
    }

    public boolean isSetCmdEnabled() {
        return setCmdEnabled;
    }

    public void setSetCmdEnabled(boolean setCmdEnabled) {
        this.setCmdEnabled = setCmdEnabled;
    }

    public String getLocalStoreLocation() {
        return localStoreLocation;
    }

    public void setLocalStoreLocation(String localStoreLocation) {
        this.localStoreLocation = localStoreLocation;
    }

    public String getLocalMaxBytesLocalHeap() {
        return localMaxBytesLocalHeap;
    }

    public void setLocalMaxBytesLocalHeap(String localMaxBytesLocalHeap) {
        this.localMaxBytesLocalHeap = localMaxBytesLocalHeap;
    }

    public String getLocalMaxBytesLocalDisk() {
        return localMaxBytesLocalDisk;
    }

    public void setLocalMaxBytesLocalDisk(String localMaxBytesLocalDisk) {
        this.localMaxBytesLocalDisk = localMaxBytesLocalDisk;
    }

    public int getLocalTimeToIdleSeconds() {
        return localTimeToIdleSeconds;
    }

    public void setLocalTimeToIdleSeconds(int localTimeToIdleSeconds) {
        this.localTimeToIdleSeconds = localTimeToIdleSeconds;
    }

    public int getLocalDiskExpiryThreadIntervalSeconds() {
        return localDiskExpiryThreadIntervalSeconds;
    }

    public void setLocalDiskExpiryThreadIntervalSeconds(int localDiskExpiryThreadIntervalSeconds) {
        this.localDiskExpiryThreadIntervalSeconds = localDiskExpiryThreadIntervalSeconds;
    }

    public int getLocalTimeToLiveSeconds() {
        return localTimeToLiveSeconds;
    }

    public void setLocalEnabled(boolean localEnabled) {
        this.localEnabled = localEnabled;
    }

    public int getFetchTimeoutSeconds() {
        return fetchTimeoutSeconds;
    }

    public void setFetchTimeoutSeconds(int fetchTimeoutSeconds) {
        this.fetchTimeoutSeconds = fetchTimeoutSeconds;
    }

}