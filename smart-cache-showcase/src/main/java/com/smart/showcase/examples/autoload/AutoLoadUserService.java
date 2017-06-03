package com.smart.showcase.examples.autoload;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.smart.showcase.common.User;

@Service
public class AutoLoadUserService {

    public static final Logger logger = LoggerFactory.getLogger(AutoLoadUserService.class);

    @Autowired
    private AutoLoadUserDao    autoLoadUserDao;

    public User get(Long id) {
        logger.debug("get");
        return autoLoadUserDao.get(id);
    }

    public User create() {
        logger.debug("create");
        return autoLoadUserDao.create();
    }

    public void delete(Long id) {
        logger.debug("delete");
        autoLoadUserDao.delete(id);
    }

    public List<User> find() {
        return autoLoadUserDao.find();
    }

}
