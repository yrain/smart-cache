package com.smart.cache.spring;

import java.util.concurrent.Callable;

import org.springframework.cache.support.SimpleValueWrapper;

import com.smart.cache.CacheTemplate;

public class Cache implements org.springframework.cache.Cache {

    private String        name;
    private CacheTemplate cacheTemplate;

    public Cache(String name, CacheTemplate cacheTemplate) {
        this.name = name;
        this.cacheTemplate = cacheTemplate;
    }

    @Override
    public ValueWrapper get(Object key) {
        return toValueWrapper(this.cacheTemplate.get(this.name, String.valueOf(key)));
    }

    @Override
    public <T> T get(Object key, Class<T> type) {
        return this.cacheTemplate.get(this.name, String.valueOf(key));
    }

    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        T value = this.cacheTemplate.get(this.name, String.valueOf(key));
        if (value == null) {
            return loadValue(key, valueLoader);
        }
        return value;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Object getNativeCache() {
        return this.cacheTemplate;
    }

    @Override
    public void put(Object key, Object value) {
        this.cacheTemplate.set(this.name, String.valueOf(key), value);
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        return toValueWrapper(this.cacheTemplate.setIfAbsent(this.name, String.valueOf(key), value));
    }

    @Override
    public void evict(Object key) {
        this.cacheTemplate.del(this.name, String.valueOf(key));
    }

    @Override
    public void clear() {
        this.cacheTemplate.rem(this.name);
    }

    private ValueWrapper toValueWrapper(Object value) {
        return (value != null ? new SimpleValueWrapper(value) : null);
    }

    private <T> T loadValue(Object key, Callable<T> valueLoader) {
        T value;
        try {
            value = valueLoader.call();
        } catch (Throwable ex) {
            throw new ValueRetrievalException(key, valueLoader, ex);
        }
        put(key, value);
        return value;
    }
}
