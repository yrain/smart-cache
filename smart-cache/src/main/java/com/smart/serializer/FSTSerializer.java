package com.smart.serializer;

import java.lang.reflect.Type;

import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FSTSerializer
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class FSTSerializer implements Serializer {

    public static final Logger            logger = LoggerFactory.getLogger(FSTSerializer.class);

    private static final FSTConfiguration conf   = FSTConfiguration.createDefaultConfiguration();

    @Override
    public byte[] serialize(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            return conf.asByteArray(obj);
        } catch (Exception e) {
            logger.error("序列化失败", e);
            return null;
        }
    }

    @Override
    public Object deserialize(byte[] bytes) {
        return deserialize(bytes, null);
    }

    public Object deserialize(byte[] bytes, Type clazz) {
        if (null == bytes || bytes.length == 0) {
            return null;
        }
        try {
            return conf.asObject(bytes);
        } catch (Exception e) {
            logger.error("反序列化失败", e);
            return null;
        }
    }

}
