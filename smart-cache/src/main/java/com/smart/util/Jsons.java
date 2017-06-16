package com.smart.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class Jsons {

    public static final SerializerFeature[] ToJSONStringFeatures = { SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue, SerializerFeature.DisableCircularReferenceDetect };

    public static final String toJSONString(Object o) {
        if (null == o) {
            return null;
        }
        return JSON.toJSONString(o, ToJSONStringFeatures);
    }

    public static <T> T parse(String jsonString, Class<T> clazz) {
        if (null == jsonString || null == clazz) {
            return null;
        } else {
            return JSON.parseObject(jsonString, clazz);
        }
    }

}
