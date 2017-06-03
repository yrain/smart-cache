package com.smart.showcase.examples.autoload;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.jarvis.cache.annotation.Cache;
import com.jarvis.cache.annotation.CacheDelete;
import com.jarvis.cache.annotation.CacheDeleteKey;
import com.jarvis.cache.type.CacheOpType;
import com.smart.showcase.common.User;
import com.smart.showcase.common.UserData;

@Component
public class AutoLoadUserDao {

    public static final String KEY_USER      = "AutoLoad.User";
    public static final String KEY_USER_FIND = "AutoLoad.User_find";
    public static final int    EXPIRE        = 600;

    @Autowired
    private UserData      userData;

    /**
     * 查询对象后并缓存
     */
    @Cache(key = KEY_USER, hfield = "#args[0]", expire = EXPIRE)
    public User get(Long id) {
        return userData.get(id);
    }

    /**
     * 创建对象后并缓存,再删除因对象集合变更后,失效的查询缓存
     */
    @Cache(key = KEY_USER, hfield = "#retVal.id", expire = EXPIRE, opType = CacheOpType.WRITE)
    @CacheDelete({ @CacheDeleteKey(value = KEY_USER_FIND) })
    public User create() {
        return userData.create();
    }

    /**
     * 删除对象,并删除其缓存,再删除以及对象集合变更后,失效的查询缓存
     */
    @CacheDelete({//
            @CacheDeleteKey(value = KEY_USER, hfield = "#args[0]", condition = "null != #args[0]"), //
            @CacheDeleteKey(value = KEY_USER_FIND) //
    })
    public void delete(Long id) {
        userData.delete(id);
    }

    /**
     * 查询对象集合并缓存
     */
    @Cache(key = KEY_USER_FIND, hfield = "no_args", expire = EXPIRE)
    public List<User> find() {
        return userData.find();
    }

    /**
     * 查询对象集合并缓存
     */
    @Cache(key = KEY_USER_FIND, hfield = "#hash(#args)", expire = EXPIRE)
    public List<User> find(String name) {
        return userData.find();
    }

}
