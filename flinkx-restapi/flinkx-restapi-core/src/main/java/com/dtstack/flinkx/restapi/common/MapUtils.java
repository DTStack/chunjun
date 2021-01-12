package com.dtstack.flinkx.restapi.common;

import com.dtstack.flinkx.util.GsonUtil;

import java.util.Map;
import java.util.Objects;

public class MapUtils {


    public static Object getData(Map<String, Object> data, String[] names) {
        Map<String, Object> tempHashMap = data;
        for (int i = 0; i < names.length; i++) {
            if (tempHashMap.containsKey(names[i]) && i != names.length - 1) {
                if (Objects.isNull(tempHashMap.get(names[i]))) {
                    return null;
                }
                if (tempHashMap.get(names[i]) instanceof Map) {
                    tempHashMap = (Map<String, Object>) tempHashMap.get(names[i]);
                } else if (tempHashMap.get(names[i]) instanceof String) {
                    try {
                        tempHashMap = GsonUtil.GSON.fromJson((String) tempHashMap.get(names[i]), GsonUtil.gsonMapTypeToken);
                    } catch (Exception e) {
                        return null;
                    }
                } else {
                    return null;
                }
            } else if (i == names.length - 1) {
                return tempHashMap.get(names[i]);
            } else {
                return null;
            }
        }
        return null;
    }
}
