package com.smart.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smart.jedis.JedisTemplate;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.JedisPubSub;

/**
 * RedisPubSubSync
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class RedisPubSubSync extends JedisPubSub implements CacheSync {

    public static final Logger logger = LoggerFactory.getLogger(RedisPubSubSync.class);

    private CacheTemplate      cacheTemplate;
    private JedisTemplate      jedisTemplate;
    private CacheSyncHandler   cacheSyncHandler;

    public RedisPubSubSync(final CacheTemplate cacheTemplate) {
        this.cacheTemplate = cacheTemplate;
        this.cacheSyncHandler = new CacheSyncHandler(this.cacheTemplate);
        this.jedisTemplate = cacheTemplate.getJedisTemplate();

        new Thread(new Runnable() {
            @Override
            public void run() {
                jedisTemplate.subscribe(new BinaryJedisPubSub() {
                    @Override
                    public void onMessage(byte[] channel, byte[] message) {
                        Command cmd = jedisTemplate.deserializeVal(message);
                        if (cmd != null) {
                            if ((cmd.src != null && !cmd.src.equals(Cache.ID)) || cmd.oper == Command.OPT_FETCH) {
                                logger.debug("recieve from " + cmd.src + " > " + cmd.toString());
                                cacheSyncHandler.handle(cmd);
                            }
                        }
                    }
                }, Cache.CACHE_STORE_SYNC);
            }
        }, "RedisPubSubSync.Subscribe").start();
    }

    @Override
    public void sendCommand(Command command) {
        this.jedisTemplate.publish(Cache.CACHE_STORE_SYNC, this.jedisTemplate.serializeVal(command));
    }

}