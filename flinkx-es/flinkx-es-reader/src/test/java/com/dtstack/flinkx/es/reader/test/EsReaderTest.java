package com.dtstack.flinkx.es.reader.test;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by softfly on 18/2/8.
 */
public class EsReaderTest {
    public static void main(String[] args) {
        Gson gson = new Gson();
        Map<String,Object> map = new HashMap<>();
        map.put("xxx", 111);
        map.put("yyyy", "fff");
        String json = gson.toJson(map);
        System.out.println(json);
    }
}
