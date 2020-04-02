package com.dtstack.flinkx.util;

import com.google.gson.internal.LinkedTreeMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2020/3/13
 */
public class MapUtilTest {

    @Test
    public void testConvertToHashMap() {
        Map<String, Object> target = new HashMap<>();
        LinkedTreeMap treeMap = new LinkedTreeMap<>();
        treeMap.put("key1", "val1");

        target.put("key11", treeMap);

        Map<String, Object> result = MapUtil.convertToHashMap(target);
        Assert.assertEquals(result, new HashMap<String, Object>() {{
            put("key11", new HashMap<String, Object>(){{
                put("key1", "val1");}
            });
        }});
    }

    @Test
    public void testObjectToMap() {
        try {
            Map<String, Object> expect = new HashMap<>();
            expect.put("key", "val");

            Map<String, Object> result = MapUtil.objectToMap(new TestObj("val"));
            Assert.assertEquals(result, expect);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    class TestObj {
        private String key;

        public TestObj(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }

    @Test
    public void testJsonStrToObject() {
        try {
            Map<String, Object> result = MapUtil.jsonStrToObject("{\"key\":\"val\"}", Map.class);
            Assert.assertEquals(result, new HashMap<String, Object>() {{
                put("key", "val");
            }});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme