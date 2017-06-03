package com.smart.cache.autoload.serializer;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Calendar;
import java.util.Date;

import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarvis.cache.serializer.ISerializer;
import com.jarvis.lib.util.BeanUtil;

public class FSTSerializer implements ISerializer<Object> {

    private static final Logger           logger = LoggerFactory.getLogger(FSTSerializer.class);

    private static final FSTConfiguration conf   = FSTConfiguration.createDefaultConfiguration();

    @Override
    public byte[] serialize(Object obj) throws Exception {
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
    public Object deserialize(byte[] bytes, Type returnType) throws Exception {
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

    @Override
    public Object deepClone(Object obj, final Type type) throws Exception {
        if (null == obj) {
            return obj;
        }
        Class<?> clazz = obj.getClass();
        if (BeanUtil.isPrimitive(obj) || clazz.isEnum() || obj instanceof Class || clazz.isAnnotation() || clazz.isSynthetic()) {// 常见不会被修改的数据类型
            return obj;
        }
        if (obj instanceof Date) {
            return ((Date) obj).clone();
        } else if (obj instanceof Calendar) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(((Calendar) obj).getTime().getTime());
            return cal;
        }
        return deserialize(serialize(obj), null);
    }

    @Override
    public Object[] deepCloneMethodArgs(Method method, Object[] args) throws Exception {
        if (null == args || args.length == 0) {
            return args;
        }
        Type[] genericParameterTypes = method.getGenericParameterTypes();
        if (args.length != genericParameterTypes.length) {
            throw new Exception("the length of " + method.getDeclaringClass().getName() + "." + method.getName() + " must " + genericParameterTypes.length);
        }
        Object[] res = new Object[args.length];
        int len = genericParameterTypes.length;
        for (int i = 0; i < len; i++) {
            res[i] = deepClone(args[i], null);
        }
        return res;
    }

}
