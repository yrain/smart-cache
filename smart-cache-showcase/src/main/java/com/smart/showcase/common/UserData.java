package com.smart.showcase.common;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.smart.jedis.JedisTemplate;

@Component
public class UserData implements InitializingBean {

    public static final Logger  logger = LoggerFactory.getLogger(User.class);

    private static final String KEY    = "testdata";

    @Autowired
    private JedisTemplate       jedisTemplate;

    @Override
    public void afterPropertiesSet() throws Exception {
        jedisTemplate.hset(KEY, 1, new User(1l, "John"));
        jedisTemplate.hset(KEY, 2, new User(2l, "Terry"));
        jedisTemplate.hset(KEY, 3, new User(3l, "Walter"));
    }

    public User get(Long id) {
        logger.debug("get");
        sleep();
        return jedisTemplate.hget(KEY, id);
    }

    public User create() {
        logger.debug("create");
        Long id = new Date().getTime();
        User user = new User(id, "name-" + String.valueOf(id));
        jedisTemplate.hset(KEY, id, user);
        return user;
    }

    public void delete(Long id) {
        logger.debug("delete");
        jedisTemplate.hdel(KEY, id);
    }

    public List<User> find() {
        logger.debug("find");
        sleep();
        return jedisTemplate.hvals(KEY);
    }

    public static void sleep() {
        try {
            logger.debug("sleep 1.5s.....");
            Thread.sleep(1500);
        } catch (Exception e) {
            // ignore
        }
    }
}
