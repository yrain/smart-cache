package com.smart.cache.autoload;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarvis.cache.AbstractCacheManager;
import com.jarvis.cache.exception.CacheCenterConnectionException;
import com.jarvis.cache.script.AbstractScriptParser;
import com.jarvis.cache.serializer.ISerializer;
import com.jarvis.cache.to.AutoLoadConfig;
import com.jarvis.cache.to.CacheKeyTO;
import com.jarvis.cache.to.CacheWrapper;
import com.smart.cache.CacheTemplate;

/**
 * CacheManager
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class CacheManager extends AbstractCacheManager {

    public static final Logger logger       = LoggerFactory.getLogger(CacheManager.class);

    private CacheTemplate      cacheTemplate;

    private String             defaultField = "_";                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             // 用"_"占位
    /**
     * Hash的缓存时长：等于0时永久缓存；大于0时，主要是为了防止一些已经不用的缓存占用内存;hashExpire小于0时，则使用@Cache中设置的expire值（默认值为-1）。
     */
    private int                hashExpire   = -1;

    public CacheManager(AutoLoadConfig config, ISerializer<Object> serializer, AbstractScriptParser scriptParser) {
        super(config, serializer, scriptParser);
    }

    public CacheManager(AutoLoadConfig config, ISerializer<Object> serializer, AbstractScriptParser scriptParser, CacheTemplate cacheTemplate) {
        super(config, serializer, scriptParser);
        this.cacheTemplate = cacheTemplate;
    }

    @Override
    public void setCache(final CacheKeyTO cacheKeyTO, final CacheWrapper<Object> result, final Method method, final Object args[]) throws CacheCenterConnectionException {
        if (null == cacheTemplate || null == cacheKeyTO) {
            return;
        }
        String cacheKey = cacheKeyTO.getCacheKey();
        if (null == cacheKey || cacheKey.length() == 0) {
            return;
        }
        try {
            int expire = result.getExpire();
            String hfield = cacheKeyTO.getHfield();
            if (null == hfield || hfield.length() == 0) {
                if (expire == 0) {
                    cacheTemplate.set(cacheKey, defaultField, result);
                } else if (expire > 0) {
                    cacheTemplate.set(cacheKey, defaultField, result, expire);
                }
            } else {
                logger.debug("set key:" + cacheKey + ",hfield:" + hfield);
                int hExpire;
                if (hashExpire < 0) {
                    hExpire = result.getExpire();
                } else {
                    hExpire = hashExpire;
                }
                if (hExpire == 0) {
                    cacheTemplate.set(cacheKey, hfield, result);
                } else if (hExpire > 0) {
                    cacheTemplate.set(cacheKey, hfield, result, hExpire);
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        } finally {
        }
    }

    @Override
    public CacheWrapper<Object> get(final CacheKeyTO cacheKeyTO, final Method method, final Object args[]) throws CacheCenterConnectionException {
        if (null == cacheTemplate || null == cacheKeyTO) {
            return null;
        }
        String cacheKey = cacheKeyTO.getCacheKey();
        if (null == cacheKey || cacheKey.length() == 0) {
            return null;
        }
        CacheWrapper<Object> res = null;
        try {
            String hfield = cacheKeyTO.getHfield();
            if (null == hfield || hfield.length() == 0) {
                res = cacheTemplate.get(cacheKey, defaultField);
            } else {
                res = cacheTemplate.get(cacheKey, hfield);
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        } finally {
        }
        return res;
    }

    /**
     * 根据缓存Key删除缓存
     * 
     * @param cacheKeyTO 缓存Key
     */
    @Override
    public void delete(CacheKeyTO cacheKeyTO) throws CacheCenterConnectionException {
        if (null == cacheTemplate || null == cacheKeyTO) {
            return;
        }
        String cacheKey = cacheKeyTO.getCacheKey();
        if (null == cacheKey || cacheKey.length() == 0) {
            return;
        }
        try {
            String hfield = cacheKeyTO.getHfield();
            if (null == hfield || hfield.length() == 0) {
                logger.debug("remove key:" + cacheKey);
                cacheTemplate.rem(cacheKey);
            } else {
                logger.debug("delete key:" + cacheKey + ",hfield:" + hfield);
                cacheTemplate.del(cacheKey, hfield);
            }
            this.getAutoLoadHandler().resetAutoLoadLastLoadTime(cacheKeyTO);
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        } finally {
        }
    }

    public int getHashExpire() {
        return hashExpire;
    }

    public void setHashExpire(int hashExpire) {
        if (hashExpire < 0) {
            return;
        }
        this.hashExpire = hashExpire;
    }

}
