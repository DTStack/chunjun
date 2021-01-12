package com.dtstack.flinkx.util;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.gson.internal.LinkedHashTreeMap;
import com.google.gson.internal.LinkedTreeMap;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reason:
 * Date: 2019/8/9
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class MapUtil {


    private static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * convert LinkedTreeMap or LinkedHashTreeMap Map to HashMap,for LinkedTreeMap,LinkedHashTreeMap can not serialize
     * @param target
     * @return
     */
    public static Map<String, Object> convertToHashMap(Map<String, Object> target){
        for(Map.Entry<String, Object> tmp : target.entrySet()){
            if (null == tmp.getValue()) {
                continue;
            }

            if(tmp.getValue().getClass().equals(LinkedTreeMap.class) ||
                    tmp.getValue().getClass().equals(LinkedHashTreeMap.class)){
                Map<String, Object> convert = convertToHashMap((Map)tmp.getValue());
                HashMap<String, Object> hashMap = new HashMap<>(convert.size());
                hashMap.putAll(convert);
                tmp.setValue(hashMap);
            }
        }

        return target;
    }


    public static Map<String,Object> objectToMap(Object obj) throws Exception{
        return objectMapper.readValue(objectMapper.writeValueAsBytes(obj), Map.class);
    }

    public static <T> T jsonStrToObject(String jsonStr, Class<T> clazz) throws JsonParseException, JsonMappingException, JsonGenerationException, IOException {
        return  objectMapper.readValue(jsonStr, clazz);
    }

    public static String writeValueAsString(Object obj) throws JsonProcessingException {
        return objectMapper.writeValueAsString(obj);
    }

}
