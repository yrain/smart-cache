package com.smart.showcase.admin;

import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.smart.cache.Cache.Level;
import com.smart.cache.CacheData;
import com.smart.cache.CacheTemplate;

@RestController
@RequestMapping("/showcase/admin/cache")
public class CacheController {

    @Autowired
    private CacheTemplate cacheTemplate;

    @RequestMapping("/init")
    public void init() {
        cacheTemplate.cls();
        cacheTemplate.set("auth", "10001", "session1");
        cacheTemplate.set("auth", "10002", "session2");
        cacheTemplate.set("auth", "10003", "session3");
        cacheTemplate.set("find", "id=1&name=a", "user1");
        cacheTemplate.set("find", "id=1&name=b", "user2");
        cacheTemplate.set("find", "id=1&name=c", "user3");
    }

    @RequestMapping("/get")
    public CacheData get(String name, String key) {
        return this.cacheTemplate.getCacheData(name, key, Level.Remote);
    }

    @RequestMapping("/del")
    public void del(String name, String key) {
        this.cacheTemplate.del(name, key);
    }

    @RequestMapping("/rem")
    public void rem(String name) {
        this.cacheTemplate.rem(name);
    }

    @RequestMapping("/cls")
    public void cls() {
        this.cacheTemplate.cls();
    }

    @RequestMapping("/names")
    public Set<String> names() {
        return this.cacheTemplate.names();
    }

    @RequestMapping("/keys")
    public Set<String> keys(String name) {
        return this.cacheTemplate.keys(name);
    }

    @RequestMapping("/fetch")
    public List<CacheData> fetch(String name, String key) {
        return this.cacheTemplate.fetch(name, key);
    }

}