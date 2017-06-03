package com.smart.showcase.examples.springcache;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.smart.showcase.common.User;
import com.smart.showcase.common.UserData;

@Component
public class SpringCacheUserDao {

    @Autowired
    private UserData userData;

    public User get(Long id) {
        return userData.get(id);
    }

    public User create() {
        return userData.create();
    }

    public void delete(Long id) {
        userData.delete(id);
    }

    public List<User> find() {
        return userData.find();
    }

}
