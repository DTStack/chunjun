package com.dtstack.chunjun.server.util;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-09-20
 */
public class JsonMapper {
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static <T> T fromJsonString(String json, Class<T> targetClass) throws IOException {
        return objectMapper.readValue(json, targetClass);
    }

    public static String writeValueAsString(Object obj) throws IOException {
        return objectMapper.writeValueAsString(obj);
    }
}
