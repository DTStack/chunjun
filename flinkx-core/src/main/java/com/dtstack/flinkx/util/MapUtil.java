package com.dtstack.flinkx.util;

import com.google.gson.internal.LinkedHashTreeMap;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

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
            if(tmp.getValue().getClass().equals(LinkedTreeMap.class) ||
                    tmp.getValue().getClass().equals(LinkedHashTreeMap.class)){
                Map<String, Object> convert = convertToHashMap((Map)tmp.getValue());
                HashMap<String, Object> hashMap = new HashMap<>();
                hashMap.putAll(convert);
                tmp.setValue(hashMap);
            }
        }

        return target;
    }


    public static Map<String,Object> ObjectToMap(Object obj) throws Exception{
        return objectMapper.readValue(objectMapper.writeValueAsBytes(obj), Map.class);
    }

}
