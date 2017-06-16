package com.smart.cache;

/**
 * Cache
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public abstract class Cache {

    static String ID;
    static String HOST;
    static String CACHE_STORE;
    static String CACHE_STORE_SYNC;

    private Cache() {
    }

    public static enum Level {
        Local, Remote
    }

    public static enum Operator {
        SET, GET, DEL, REM, CLS
    }

}
