package com.dtstack.flinkx.util;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.gson.internal.LinkedHashTreeMap;
import com.google.gson.internal.LinkedTreeMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.dtstack.flinkx.util.StringUtil.escapeExprSpecialWord;

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

    /**
     * 根据key 以及切割键 获取真正的key，将key 和value放入data中
     * @param key key
     * @param fieldDelimiter 切割键
     * @param value 值
     * @param data 载体data
     */
    public static void buildMap(String key, String fieldDelimiter, Object value, Map<String, Object> data) {
        String[] split = new String[1];
        if (StringUtils.isBlank(fieldDelimiter)) {
            split[0] = key;
        } else {
            split = key.split(escapeExprSpecialWord(fieldDelimiter));
        }

        if (split.length == 1) {
            data.put(split[0], value);
        } else {
            Map<String, Object> temp = data;
            for (int i = 0; i < split.length - 1; i++) {
                if (temp.containsKey(split[i])) {
                    if (temp.get(split[i]) instanceof HashMap) {
                        temp = (HashMap) temp.get(split[i]);
                    } else {
                        throw new RuntimeException("build map failed ,data is " + GsonUtil.GSON.toJson(data) + " key is " + key);
                    }
                } else {
                    Map hashMap = new HashMap(2);
                    temp.put(split[i], hashMap);
                    temp = hashMap;
                }
                if (i == split.length - 2) {
                    temp.put(split[split.length - 1], value);
                }
            }
        }
    }


    /**
     * 根据指定的key从map里获取对应的值
     * 如果key不存在 报错
     *
     * @param map            需要解析的map
     * @param key            指定的key  key可以是嵌套的
     * @param fieldDelimiter 嵌套key的分隔符
     */
    public static Object getValueByKey(Map<String, Object> map, String key, String fieldDelimiter) {
        if (MapUtils.isEmpty(map)) {
            throw new RuntimeException(key + " not exist  because map is empty");
        }
        Object o = null;
        String[] split = new String[1];
        if (StringUtils.isBlank(fieldDelimiter)) {
            split[0] = key;
        } else {
            split = key.split(escapeExprSpecialWord(fieldDelimiter));
        }

        Map<String, Object> tempMap = map;
        for (int i = 0; i < split.length; i++) {
            o = getValue(tempMap, split[i]);
            //仅仅代表这个key对应的值是null但是key还是存在的
            if (o == null && i != split.length - 1) {
                throw new RuntimeException(key + " on  [" + GsonUtil.GSON.toJson(map) + "]  is null");
            }

            if (i != split.length - 1) {
                if (!(o instanceof Map)) {
                    throw new RuntimeException("key " + key + " on " + map + " is not a json");
                }
                tempMap = (Map<String, Object>) o;
            }
        }
        return o;
    }


    private static Object getValue(Map<String, Object> map, String key) {
        if (!map.containsKey(key)) {
            throw new RuntimeException(key + " not exist on  " + GsonUtil.GSON.toJson(map) );
        }
        return map.get(key);
    }

}
