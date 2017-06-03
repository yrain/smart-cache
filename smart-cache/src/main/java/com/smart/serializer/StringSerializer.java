package com.smart.serializer;

/**
 * StringSerializer
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class StringSerializer implements Serializer {

    public byte[] serialize(Object string) {
        return (string == null ? null : (String.valueOf(string)).getBytes(CHARSET));
    }

    public Object deserialize(byte[] bytes) {
        return (bytes == null ? null : new String(bytes, CHARSET));
    }

}
