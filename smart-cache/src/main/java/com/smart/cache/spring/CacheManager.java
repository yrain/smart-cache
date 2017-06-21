package com.smart.cache.spring;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.cache.transaction.AbstractTransactionSupportingCacheManager;

import com.smart.cache.CacheTemplate;

/**
 * CacheManager
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class CacheManager extends AbstractTransactionSupportingCacheManager {

    private CacheTemplate cacheTemplate;

    public CacheManager() {
    }

    public CacheManager(CacheTemplate cacheTemplate) {
        this.cacheTemplate = cacheTemplate;
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
    }

    @Override
    protected Collection<org.springframework.cache.Cache> loadCaches() {
        Set<String> names = this.cacheTemplate.names();
        Collection<org.springframework.cache.Cache> caches = new LinkedHashSet<org.springframework.cache.Cache>(names.size());
        for (String name : names) {
            caches.add(new Cache(name, this.cacheTemplate));
        }
        return caches;
    }

    @Override
    protected org.springframework.cache.Cache getMissingCache(String name) {
        return new Cache(name, this.cacheTemplate);
    }

    public CacheTemplate getCacheTemplate() {
        return cacheTemplate;
    }

    public void setCacheTemplate(CacheTemplate cacheTemplate) {
        this.cacheTemplate = cacheTemplate;
    }

}
