package com.smart.showcase.examples.autoload;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.smart.showcase.common.User;

@RestController
@RequestMapping("/showcase/autoload/user")
public class AutoLoadUserController {

    @Autowired
    private AutoLoadUserService autoLoadUserService;

    @GetMapping("/get")
    public User get(Long id) {
        return autoLoadUserService.get(id);
    }

    @GetMapping("/create")
    public User create() {
        return autoLoadUserService.create();
    }

    @GetMapping("/delete")
    public void delete(Long id) {
        autoLoadUserService.delete(id);
    }

    @GetMapping("/find")
    public List<User> find() {
        return autoLoadUserService.find();
    }

}
