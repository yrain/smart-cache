package com.smart.showcase.examples.springcache;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.stereotype.Service;

import com.smart.showcase.common.User;

@Service
public class SpringCacheUserService {

    public static final Logger logger        = LoggerFactory.getLogger(SpringCacheUserService.class);
    public static final String KEY_USER      = "SpringCache.User";
    public static final String KEY_USER_FIND = "SpringCache.User_find";
    @Autowired
    private SpringCacheUserDao userDao;

    @Cacheable(value = KEY_USER, key = "#id")
    public User get(Long id) {
        return userDao.get(id);
    }

    @CacheEvict(value = KEY_USER_FIND, allEntries = true)
    public User create() {
        return userDao.create();
    }

    @Caching(//
            evict = { //
                    @CacheEvict(value = KEY_USER, key = "#id"), //
                    @CacheEvict(value = KEY_USER_FIND, allEntries = true) //
            })
    public void delete(Long id) {
        userDao.delete(id);
    }

    @Cacheable(value = KEY_USER_FIND)
    public List<User> find() {
        return userDao.find();
    }

    @Cacheable(value = KEY_USER_FIND, key = "#name")
    public List<User> find(String name) {
        return userDao.find();
    }

}
