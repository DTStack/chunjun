/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.conf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperatorConfTest {
    private OperatorConf operatorConf;

    @BeforeEach
    public void setUp() {
        operatorConf = new OperatorConf();
        String key = "key";
        Map<String, Object> map = new HashMap<>();
        map.put(key, new HashMap<>());
        operatorConf.setParameter(map);
    }

    @Test
    @DisplayName("Should set the fieldnamelist")
    public void setFieldNameListWhenFieldListIsNotNull() {
        List<String> fieldNameList = new ArrayList<>();
        fieldNameList.add("test");
        operatorConf.setFieldNameList(fieldNameList);
        assertNotNull(operatorConf.getFieldNameList());
    }

    @Test
    @DisplayName("Should return the properties when the key is found")
    public void getPropertiesWhenKeyIsFoundThenReturnProperties() {
        String key = "key";
        Properties properties = operatorConf.getProperties(key, null);
        assertNotNull(properties);
    }

    @Test
    @DisplayName("Should return defaultvalue when the key is not found")
    public void getBooleanValWhenKeyIsNotFoundThenReturnDefaultValue() {
        String key = "key1";
        boolean defaultValue = true;
        boolean actual = operatorConf.getBooleanVal(key, defaultValue);
        assertEquals(defaultValue, actual);
    }

    @Test
    @DisplayName("Should return the value when the key is found")
    public void getBooleanValWhenKeyIsFoundThenReturnTheValue() {
        Map<String, Object> parameter = new HashMap<>();
        parameter.put("key2", true);
        operatorConf.setParameter(parameter);

        boolean result = operatorConf.getBooleanVal("key2", false);

        assertTrue(result);
    }

    @Test
    @DisplayName("Should return the value of the key when the key exists")
    public void getStringValWhenKeyExistsThenReturnValue() {
        String key = "key3";
        String value = "value";
        operatorConf.getParameter().put(key, value);

        String result = operatorConf.getStringVal(key);

        assertNotNull(result);
        assertEquals(value, result);
    }

    @Test
    @DisplayName("Should return default value when the key does not exist")
    public void getStringValWhenKeyDoesNotExistThenReturnDefaultValue() {
        String key = "key4";
        String defaultValue = "defaultValue";

        String actual = operatorConf.getStringVal(key, defaultValue);

        assertEquals(defaultValue, actual);
    }

    @Test
    @DisplayName("Should return the default value when the key is not in the map")
    public void getLongValWhenKeyIsNotInMapThenReturnDefaultValue() {
        OperatorConf operatorConf = new OperatorConf();
        operatorConf.setParameter(new HashMap<>());

        long result = operatorConf.getLongVal("key5", 1L);

        assertEquals(1L, result);
    }

    @Test
    @DisplayName("Should return the value when the key is in the map and it's a string")
    public void getLongValWhenKeyIsInMapAndItIsAStringThenReturnValue() {
        Map<String, Object> map = new HashMap<>();
        map.put("key6", "1");
        OperatorConf operatorConf = new OperatorConf();
        operatorConf.setParameter(map);

        long result = operatorConf.getLongVal("key6", 0);

        assertEquals(1, result);
    }

    @Test
    @DisplayName("Should return the default value when the key is not in the map")
    public void getIntValWhenKeyIsNotInTheMapThenReturnDefaultValue() {
        Map<String, Object> map = new HashMap<>();
        OperatorConf operatorConf = new OperatorConf();
        operatorConf.setParameter(map);

        int result = operatorConf.getIntVal("key7", 1);

        assertEquals(1, result);
    }

    @Test
    @DisplayName("Should return the value when the key is in the map and it's an integer")
    public void getIntValWhenKeyIsInTheMapAndItIsAnIntegerThenReturnValue() {
        Map<String, Object> parameter = new HashMap<>();
        parameter.put("key8", 1);
        OperatorConf operatorConf = new OperatorConf();
        operatorConf.setParameter(parameter);

        int result = operatorConf.getIntVal("key8", 0);

        assertEquals(1, result);
    }

    @Test
    @DisplayName("Should return the value when the key is in the map")
    public void getValWhenKeyIsInTheMap() {
        operatorConf.setParameter(
                new HashMap<String, Object>() {
                    {
                        put("key9", "value");
                    }
                });

        assertEquals("value", operatorConf.getVal("key9"));
    }

    @Test
    @DisplayName("Should return the fieldnamelist when the fieldnamelist is not null")
    public void getFieldNameListWhenFieldNameListIsNotNull() {
        OperatorConf operatorConf = new OperatorConf();
        List<String> fieldNameList = new ArrayList<>();
        fieldNameList.add("field1");
        fieldNameList.add("field2");
        operatorConf.setFieldNameList(fieldNameList);
        assertNotNull(operatorConf.getFieldNameList());
    }

    @Test
    @DisplayName("Should set the parameter when the parameter is not null")
    public void setParameterWhenParameterIsNotNull() {
        Map<String, Object> parameter = new HashMap<>();
        operatorConf.setParameter(parameter);
        assertNotNull(operatorConf.getParameter());
    }

    @Test
    @DisplayName("Should set the table when the table is not null")
    public void setTableWhenTableIsNotNull() {
        OperatorConf operatorConf = new OperatorConf();
        TableConf tableConf = new TableConf();

        operatorConf.setTable(tableConf);

        assertNotNull(operatorConf.getTable());
    }

    @Test
    @DisplayName("Should set the semantic when the semantic is not null")
    public void setSemanticWhenSemanticIsNotNull() {
        OperatorConf operatorConf = new OperatorConf();
        String semantic = "semantic";

        operatorConf.setSemantic(semantic);

        assertEquals(semantic, operatorConf.getSemantic());
    }

    @Test
    @DisplayName("Should set the name when the name is not null")
    public void setNameWhenNameIsNotNull() {
        operatorConf.setName("test");
        assertEquals("test", operatorConf.getName());
    }
}
