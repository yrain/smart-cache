package com.smart.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smart.cache.Cache.Level;
import com.smart.util.Objects;

/**
 * CacheSyncHandler
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class CacheSyncHandler {

    public static final Logger logger = LoggerFactory.getLogger(CacheSyncHandler.class);

    private CacheTemplate      cacheTemplate;

    public CacheSyncHandler(CacheTemplate cacheTemplate) {
        this.cacheTemplate = cacheTemplate;
    }

    private void onSet(String name, String key) {
        logger.debug("onSet > " + name + "." + key.toString());
        this.cacheTemplate.get(name, key, Level.Remote);
    }

    private void onDel(String name, String key) {
        logger.debug("onDel > " + name + "." + key.toString());
        this.cacheTemplate.del(name, key, Level.Local);
    }

    private void onRem(String name) {
        logger.debug("onRem > " + name);
        this.cacheTemplate.rem(name, Level.Local);
    }

    private void onCls() {
        logger.debug("onCls");
        this.cacheTemplate.cls(Level.Local);
    }

    private void onFetch(String name, String key, String fetch) {
        logger.debug("onFetch > " + name + "." + key.toString() + "," + "fetch:" + fetch);
        boolean isExists = this.cacheTemplate.isExists(name, key, Level.Local);
        if (isExists) {
            Object value = this.cacheTemplate.get(name, key, Level.Local);
            int ttl = this.cacheTemplate.ttl(name, key, Level.Local);
            int tti = this.cacheTemplate.tti(name, key);
            this.cacheTemplate.getJedisTemplate().hset(fetch, Cache.ID, new CacheData(name, key, Objects.toString(value), tti, ttl, Level.Local), cacheTemplate.getFetchTimeoutSeconds());
        } else {
            this.cacheTemplate.getJedisTemplate().hset(fetch, Cache.ID, new CacheData(name, key, null, -1, -1, Level.Local), cacheTemplate.getFetchTimeoutSeconds());
        }
    }

    public void handle(Command cmd) {
        switch (cmd.oper) {
            case Command.OPT_SET:
                onSet(cmd.name, cmd.key);
                break;
            case Command.OPT_DEL:
                onDel(cmd.name, cmd.key);
                break;
            case Command.OPT_REM:
                onRem(cmd.name);
                break;
            case Command.OPT_CLS:
                onCls();
                break;
            case Command.OPT_FETCH:
                onFetch(cmd.name, cmd.key, cmd.fetch);
                break;
            default:
                logger.warn("Unknown message type = " + cmd.oper);
        }
    }

}
