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

package com.dtstack.chunjun.config;

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

class OperatorConfigTest {
    private OperatorConfig operatorConfig;

    @BeforeEach
    public void setUp() {
        operatorConfig = new OperatorConfig();
        String key = "key";
        Map<String, Object> map = new HashMap<>();
        map.put(key, new HashMap<>());
        operatorConfig.setParameter(map);
    }

    @Test
    @DisplayName("Should set the fieldnamelist")
    public void setFieldNameListWhenFieldListIsNotNull() {
        List<String> fieldNameList = new ArrayList<>();
        fieldNameList.add("test");
        operatorConfig.setFieldNameList(fieldNameList);
        assertNotNull(operatorConfig.getFieldNameList());
    }

    @Test
    @DisplayName("Should return the properties when the key is found")
    public void getPropertiesWhenKeyIsFoundThenReturnProperties() {
        String key = "key";
        Properties properties = operatorConfig.getProperties(key, null);
        assertNotNull(properties);
    }

    @Test
    @DisplayName("Should return defaultvalue when the key is not found")
    public void getBooleanValWhenKeyIsNotFoundThenReturnDefaultValue() {
        String key = "key1";
        boolean defaultValue = true;
        boolean actual = operatorConfig.getBooleanVal(key, defaultValue);
        assertEquals(defaultValue, actual);
    }

    @Test
    @DisplayName("Should return the value when the key is found")
    public void getBooleanValWhenKeyIsFoundThenReturnTheValue() {
        Map<String, Object> parameter = new HashMap<>();
        parameter.put("key2", true);
        operatorConfig.setParameter(parameter);

        boolean result = operatorConfig.getBooleanVal("key2", false);

        assertTrue(result);
    }

    @Test
    @DisplayName("Should return the value of the key when the key exists")
    public void getStringValWhenKeyExistsThenReturnValue() {
        String key = "key3";
        String value = "value";
        operatorConfig.getParameter().put(key, value);

        String result = operatorConfig.getStringVal(key);

        assertNotNull(result);
        assertEquals(value, result);
    }

    @Test
    @DisplayName("Should return default value when the key does not exist")
    public void getStringValWhenKeyDoesNotExistThenReturnDefaultValue() {
        String key = "key4";
        String defaultValue = "defaultValue";

        String actual = operatorConfig.getStringVal(key, defaultValue);

        assertEquals(defaultValue, actual);
    }

    @Test
    @DisplayName("Should return the default value when the key is not in the map")
    public void getLongValWhenKeyIsNotInMapThenReturnDefaultValue() {
        OperatorConfig operatorConfig = new OperatorConfig();
        operatorConfig.setParameter(new HashMap<>());

        long result = operatorConfig.getLongVal("key5", 1L);

        assertEquals(1L, result);
    }

    @Test
    @DisplayName("Should return the value when the key is in the map and it's a string")
    public void getLongValWhenKeyIsInMapAndItIsAStringThenReturnValue() {
        Map<String, Object> map = new HashMap<>();
        map.put("key6", "1");
        OperatorConfig operatorConfig = new OperatorConfig();
        operatorConfig.setParameter(map);

        long result = operatorConfig.getLongVal("key6", 0);

        assertEquals(1, result);
    }

    @Test
    @DisplayName("Should return the default value when the key is not in the map")
    public void getIntValWhenKeyIsNotInTheMapThenReturnDefaultValue() {
        Map<String, Object> map = new HashMap<>();
        OperatorConfig operatorConfig = new OperatorConfig();
        operatorConfig.setParameter(map);

        int result = operatorConfig.getIntVal("key7", 1);

        assertEquals(1, result);
    }

    @Test
    @DisplayName("Should return the value when the key is in the map and it's an integer")
    public void getIntValWhenKeyIsInTheMapAndItIsAnIntegerThenReturnValue() {
        Map<String, Object> parameter = new HashMap<>();
        parameter.put("key8", 1);
        OperatorConfig operatorConfig = new OperatorConfig();
        operatorConfig.setParameter(parameter);

        int result = operatorConfig.getIntVal("key8", 0);

        assertEquals(1, result);
    }

    @Test
    @DisplayName("Should return the fieldnamelist when the fieldnamelist is not null")
    public void getFieldNameListWhenFieldNameListIsNotNull() {
        OperatorConfig operatorConfig = new OperatorConfig();
        List<String> fieldNameList = new ArrayList<>();
        fieldNameList.add("field1");
        fieldNameList.add("field2");
        operatorConfig.setFieldNameList(fieldNameList);
        assertNotNull(operatorConfig.getFieldNameList());
    }

    @Test
    @DisplayName("Should set the parameter when the parameter is not null")
    public void setParameterWhenParameterIsNotNull() {
        Map<String, Object> parameter = new HashMap<>();
        operatorConfig.setParameter(parameter);
        assertNotNull(operatorConfig.getParameter());
    }

    @Test
    @DisplayName("Should set the table when the table is not null")
    public void setTableWhenTableIsNotNull() {
        OperatorConfig operatorConfig = new OperatorConfig();
        TableConfig tableConfig = new TableConfig();

        operatorConfig.setTable(tableConfig);

        assertNotNull(operatorConfig.getTable());
    }

    @Test
    @DisplayName("Should set the semantic when the semantic is not null")
    public void setSemanticWhenSemanticIsNotNull() {
        OperatorConfig operatorConfig = new OperatorConfig();
        String semantic = "semantic";

        operatorConfig.setSemantic(semantic);

        assertEquals(semantic, operatorConfig.getSemantic());
    }

    @Test
    @DisplayName("Should set the name when the name is not null")
    public void setNameWhenNameIsNotNull() {
        operatorConfig.setName("test");
        assertEquals("test", operatorConfig.getName());
    }
}
