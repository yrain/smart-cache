package com.smart.serializer;

import java.nio.charset.Charset;

/**
 * Serializer
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public interface Serializer {

    public static final Charset CHARSET = Charset.forName("UTF-8");

    public byte[] serialize(Object object);

    public Object deserialize(byte[] bytes);

}
