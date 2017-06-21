package com.smart.cache;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Set;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.base.Optional;
import com.smart.cache.Cache.Level;
import com.smart.util.Objects;

/**
 * App
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class App {

    /**
     * 启动后输入命令后回车
     * Cmd:
     * // set user 1 John
     * // get user 1
     * // del user 1
     * // rem user
     * // cls
     * // ttl user 1
     * // tti user 1
     * // fetch user 1
     * // names
     * // keys user
     * // values user
     */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:/spring.xml");
        CacheTemplate cacheTemplate = (CacheTemplate) context.getBean("cacheTemplate");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        do {
            try {
                System.out.print("> ");
                System.out.flush();

                String line = in.readLine().trim();
                if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit"))
                    break;
                String[] cmds = line.split(" ");

                // set user 1 John
                if ("set".equalsIgnoreCase(cmds[0])) {
                    if (cmds.length <= 4) {
                        cacheTemplate.set(cmds[1], cmds[2], cmds[3]);
                    } else {
                        cacheTemplate.set(cmds[1], cmds[2], cmds[3], Integer.parseInt(cmds[4]));
                    }
                    System.out.printf("set %s.%s=%s\n", cmds[1], cmds[2], cmds[3]);
                }
                // get user 1
                else if ("get".equalsIgnoreCase(cmds[0])) {
                    String v = Objects.toString(Optional.fromNullable(cacheTemplate.get(cmds[1], cmds[2])).orNull());
                    System.out.printf("get %s.%s=%s\n", cmds[1], cmds[2], v);
                }
                // del user 1
                else if ("del".equalsIgnoreCase(cmds[0])) {
                    cacheTemplate.del(cmds[1], cmds[2]);
                    System.out.printf("del %s.%s\n", cmds[1], cmds[2]);
                }
                // rem user
                else if ("rem".equalsIgnoreCase(cmds[0])) {
                    cacheTemplate.rem(cmds[1]);
                    System.out.printf("rem %s\n", cmds[1]);
                }
                // cls
                else if ("cls".equalsIgnoreCase(cmds[0])) {
                    System.out.printf("cls \n");
                    cacheTemplate.cls();
                }
                // ttl user 1
                else if ("ttl".equalsIgnoreCase(cmds[0])) {
                    System.out.printf("local  ttl %s.%s=%s\n", cmds[1], cmds[2], cacheTemplate.ttl(cmds[1], cmds[2], Level.Local));
                    System.out.printf("remote ttl %s.%s=%s\n", cmds[1], cmds[2], cacheTemplate.ttl(cmds[1], cmds[2], Level.Remote));
                }
                // fetch user 1
                else if ("fetch".equalsIgnoreCase(cmds[0])) {
                    String v = Objects.toString(Optional.fromNullable(cacheTemplate.fetch(cmds[1], cmds[2])).orNull());
                    System.out.printf("fetch %s.%s=%s\n", cmds[1], cmds[2], v);
                }
                // names
                else if ("names".equalsIgnoreCase(cmds[0])) {
                    Set<String> names = cacheTemplate.names();
                    System.out.println(Objects.toString(names));
                }
                // keys user
                else if ("keys".equalsIgnoreCase(cmds[0])) {
                    Set<String> keys = cacheTemplate.keys(cmds[1]);
                    System.out.println(cmds[1] + ":" + Objects.toString(keys));
                }
                // values user
                else if ("values".equalsIgnoreCase(cmds[0])) {
                    List<String> values = cacheTemplate.values(String.valueOf(cmds[1]));
                    System.out.println(cmds[1] + ":" + Objects.toString(values));
                }
                // test.ttl
                else if ("test.ttl".equalsIgnoreCase(cmds[0])) {
                    if (cmds.length <= 1) {
                        cacheTemplate.set("test.ttl", "test.ttl", "test.ttl");
                    } else {
                        cacheTemplate.set("test.ttl", "test.ttl", "test.ttl", Integer.parseInt(cmds[1]));
                    }
                    int i = 1;
                    while (true) {
                        Thread.sleep(1000);
                        System.out.println("------------------------------------------");
                        System.out.println(i++);
                        System.out.println(Objects.toString(cacheTemplate.get("test.ttl", "test.ttl")));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while (true);
        System.exit(0);
    }
}
