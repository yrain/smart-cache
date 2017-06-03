package com.smart.cache;

import java.io.Serializable;

/**
 * Command
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class Command implements Serializable {

    private static final long serialVersionUID  = 7126530485423286910L;

    // 设置本地缓存
    public final static byte  OPT_SET           = 0x01;
    // 删除本地缓存Key
    public final static byte  OPT_DEL           = 0x02;
    // 删除本地缓存
    public final static byte  OPT_REM           = 0x03;
    // 清空本地缓存
    public final static byte  OPT_CLS           = 0x04;
    // 获取多机本地缓存Key
    public final static byte  OPT_FETCH         = 0x05;
    // 失效后更新，即从多级缓存中拿出数据重新设置
    public final static byte  OPT_EXPIRE_UPDATE = 0x10;
    // 失效后删除
    public final static byte  OPT_EXPIRE_DELETE = 0x11;

    public byte               oper;
    public String             name;
    public String             key;
    public String             src;
    public String             fetch;

    public Command() {
    }

    public Command(byte oper, String name, String key) {
        this.oper = oper;
        this.name = name;
        this.key = key;
        this.src = Cache.ID;
    }

    public Command(byte operator, String name, String key, String fetch) {
        this(operator, name, key);
        this.fetch = fetch;
    }

    public static Command set(String cacheName, String key) {
        return new Command(OPT_SET, cacheName, key);
    }

    public static Command del(String cacheName, String key) {
        return new Command(OPT_DEL, cacheName, key);
    }

    public static Command rem(String cacheName) {
        return new Command(OPT_REM, cacheName, null);
    }

    public static Command cls() {
        return new Command(OPT_CLS, null, null);
    }

    public static Command fetch(String cacheName, String key, String fetch) {
        return new Command(OPT_FETCH, cacheName, key, fetch);
    }

    @Override
    public String toString() {
        String opt = "";
        switch (this.oper) {
            case Command.OPT_SET:
                opt = "set";
                break;
            case Command.OPT_DEL:
                opt = "del";
                break;
            case Command.OPT_REM:
                opt = "rem";
                break;
            case Command.OPT_CLS:
                opt = "cls";
                break;
            case Command.OPT_FETCH:
                opt = "fetch";
                break;
            default:
                opt = "unknown";
        }
        return "Command [oper=" + opt + ", name=" + name + ", key=" + key + "]";
    }

}