package com.smart.showcase.examples.springcache;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.smart.showcase.common.User;

@RestController
@RequestMapping("/showcase/springcache/user")
public class SpringCacheUserController {

    @Autowired
    private SpringCacheUserService springCacheUserService;

    @GetMapping("/create")
    public User create() {
        return springCacheUserService.create();
    }

    @GetMapping("/find")
    public List<User> find() {
        return springCacheUserService.find();
    }

    @GetMapping("/get")
    public User get(Long id) {
        return springCacheUserService.get(id);
    }

    @GetMapping("/delete")
    public void delete(Long id) {
        springCacheUserService.delete(id);
    }

}
