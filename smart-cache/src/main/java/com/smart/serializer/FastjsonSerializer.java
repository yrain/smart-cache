package com.smart.serializer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

/**
 * FastjsonSerializer
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class FastjsonSerializer implements Serializer {

    public static final SerializerFeature[] features = { //
            SerializerFeature.PrettyFormat//
            , SerializerFeature.WriteMapNullValue//
            , SerializerFeature.DisableCircularReferenceDetect//
            , SerializerFeature.WriteClassName// 指定序列化时写入类名，否则反序列化得不到正确的类型
            , SerializerFeature.WriteDateUseDateFormat };

    @Override
    public byte[] serialize(final Object obj) {
        if (null == obj) {
            return null;
        }
        String json = JSON.toJSONString(obj, features);
        return json.getBytes(CHARSET);
    }

    @Override
    public Object deserialize(byte[] bytes) {
        if (null == bytes || bytes.length == 0) {
            return null;
        }
        return parse(bytes, null);
    }

    public String serializeToJSONString(final Object obj) {
        if (null == obj) {
            return null;
        }
        return JSON.toJSONString(obj, features);
    }

    public <T> T deserializeFromJSONString(final String jsonString) {
        return parse(jsonString);
    }

    //
    // parse
    // ---------------------------------------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    public static <T> T parse(String jsonString) {
        if (null == jsonString) {
            return null;
        }
        return (T) JSON.parse(jsonString);
    }

    public static <T> T parse(final byte[] bytes, Class<T> clazz) {
        String json = new String(bytes, CHARSET);
        return parse(json);
    }

}