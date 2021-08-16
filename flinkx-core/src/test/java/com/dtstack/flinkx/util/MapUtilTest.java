/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.util;

import com.google.gson.internal.LinkedTreeMap;
import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertEquals(
                result,
                new HashMap<String, Object>() {
                    {
                        put(
                                "key11",
                                new HashMap<String, Object>() {
                                    {
                                        put("key1", "val1");
                                    }
                                });
                    }
                });
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
            Assert.assertEquals(
                    result,
                    new HashMap<String, Object>() {
                        {
                            put("key", "val");
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

// Generated with love by TestMe :) Please report issues and submit feature requests at:
// http://weirddev.com/forum#!/testme
