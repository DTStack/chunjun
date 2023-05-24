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

import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FieldConfigTest {

    @Test
    @DisplayName("Should return an empty list when the fieldlist is empty")
    public void getFieldListWhenFieldListIsEmptyThenReturnEmptyList() {
        List<FieldConfig> fieldList = FieldConfig.getFieldList(Collections.emptyList());
        assertNotNull(fieldList);
        assertTrue(fieldList.isEmpty());
    }

    /** Should return the correct string when all fields are not null */
    @Test
    public void toStringWhenAllFieldsAreNotNull() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setName("name");
        fieldConfig.setType(TypeConfig.of("type"));
        fieldConfig.setIndex(1);
        fieldConfig.setValue("value");
        fieldConfig.setFormat("format");
        fieldConfig.setParseFormat("parseFormat");
        fieldConfig.setSplitter("splitter");
        fieldConfig.setIsPart(true);
        fieldConfig.setNotNull(true);

        String expected =
                "FieldConfig(name=name, scriptType=TypeConfig{type='TYPE', precision=null, scale=null}, index=1, value=value, format=format, parseFormat=parseFormat, splitter=splitter, isPart=true, notNull=true)";

        assertEquals(expected, fieldConfig.toString());
    }

    /** Should return a FieldConfig object when the map is not empty */
    @Test
    public void getFieldWhenMapIsNotEmptyThenReturnFieldConfObject() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "name");
        map.put("type", "type");
        map.put("index", 1);
        map.put("value", "value");
        map.put("format", "format");
        map.put("parseFormat", "parseFormat");
        map.put("splitter", "splitter");
        map.put("isPart", true);
        map.put("notNull", true);
        map.put("length", 1);
        map.put("customConverterClass", "customConverterClass");
        map.put("customConverterType", "customConverterType");

        FieldConfig fieldConfig = FieldConfig.getField(map, 1);

        assertEquals(fieldConfig.getName(), "name");
        assertEquals(fieldConfig.getType().getType(), "type".toUpperCase(Locale.ROOT));
        assertEquals(fieldConfig.getIndex(), Integer.valueOf(1));
        assertEquals(fieldConfig.getValue(), "value");
        assertEquals(fieldConfig.getFormat(), "format");
        assertEquals(fieldConfig.getParseFormat(), "parseFormat");
        assertEquals(fieldConfig.getSplitter(), "splitter");
        assertTrue(fieldConfig.getIsPart());
        assertTrue(fieldConfig.getNotNull());
    }

    /** Should return a FieldConfig object when the map is empty */
    @Test
    public void getFieldWhenMapIsEmptyThenReturnFieldConfObject() {
        Map<String, Object> map = new HashMap<>();
        FieldConfig fieldConfig = FieldConfig.getField(map, 0);
        assertNotNull(fieldConfig);
    }

    /** Should return the parseFormat when the parseFormat is not null */
    @Test
    public void getParseFormatWhenParseFormatIsNotNull() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setParseFormat("yyyy-MM-dd HH:mm:ss");
        assertEquals("yyyy-MM-dd HH:mm:ss", fieldConfig.getParseFormat());
    }

    /** Should return null when the parseFormat is null */
    @Test
    public void getParseFormatWhenParseFormatIsNull() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setParseFormat(null);
        assertNull(fieldConfig.getParseFormat());
    }

    /** Should return the value when the value is not null */
    @Test
    public void getValueWhenValueIsNotNull() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setValue("value");
        assertEquals("value", fieldConfig.getValue());
    }

    /** Should return null when the value is null */
    @Test
    public void getValueWhenValueIsNull() {
        FieldConfig fieldConfig = new FieldConfig();
        assertNull(fieldConfig.getValue());
    }

    /** Should return the format when the format is not null */
    @Test
    public void getFormatWhenFormatIsNotNull() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setFormat("yyyy-MM-dd HH:mm:ss");
        assertEquals("yyyy-MM-dd HH:mm:ss", fieldConfig.getFormat());
    }

    /** Should return null when the format is null */
    @Test
    public void getFormatWhenFormatIsNull() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setFormat(null);
        assertNull(fieldConfig.getFormat());
    }

    /** Should return the splitter when the splitter is not null */
    @Test
    public void getSplitterWhenSplitterIsNotNull() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setSplitter("splitter");
        assertEquals("splitter", fieldConfig.getSplitter());
    }

    /** Should return null when the splitter is null */
    @Test
    public void getSplitterWhenSplitterIsNull() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setSplitter(null);
        assertNull(fieldConfig.getSplitter());
    }

    /** Should return true when the notNull is true */
    @Test
    public void getNotNullWhenNotNullIsTrue() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setNotNull(true);
        assertTrue(fieldConfig.getNotNull());
    }

    /** Should return false when the notNull is false */
    @Test
    public void getNotNullWhenNotNullIsFalse() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setNotNull(false);
        assertFalse(fieldConfig.getNotNull());
    }

    /** Should return true when the field is part */
    @Test
    public void getPartWhenFieldIsPart() {
        FieldConfig field = new FieldConfig();
        field.setIsPart(true);
        assertTrue(field.getIsPart());
    }

    /** Should return false when the field is not part */
    @Test
    public void getPartWhenFieldIsNotPart() {
        FieldConfig field = new FieldConfig();
        field.setIsPart(false);
        assertFalse(field.getIsPart());
    }

    /** Should return null when the fieldList is empty */
    @Test
    public void getSameNameMetaColumnWhenFieldListIsEmptyThenReturnNull() {
        List<FieldConfig> fieldList = new ArrayList<>();
        FieldConfig fieldConfig = FieldConfig.getSameNameMetaColumn(fieldList, "name");
        assertNull(fieldConfig);
    }

    /**
     * Should return null when the fieldList is not empty but there is no field with the same name
     */
    @Test
    public void
            getSameNameMetaColumnWhenFieldListIsNotEmptyButThereIsNoFieldWithTheSameNameThenReturnNull() {
        List<FieldConfig> fieldList = new ArrayList<>();
        FieldConfig field1 = new FieldConfig();
        field1.setName("name1");
        fieldList.add(field1);
        FieldConfig field2 = new FieldConfig();
        field2.setName("name2");
        fieldList.add(field2);
        FieldConfig field3 = new FieldConfig();
        field3.setName("name3");
        fieldList.add(field3);

        String name = "name4";

        FieldConfig result = FieldConfig.getSameNameMetaColumn(fieldList, name);

        assertNull(result);
    }

    /** Should return the index when the index is not null */
    @Test
    public void getIndexWhenIndexIsNotNull() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setIndex(1);
        assertEquals(1, fieldConfig.getIndex().intValue());
    }

    /** Should return the name of the field */
    @Test
    public void getNameShouldReturnTheNameOfTheField() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setName("name");
        assertEquals("name", fieldConfig.getName());
    }

    /** Should return the type when the type is not null */
    @Test
    public void getTypeWhenTypeIsNotNull() {
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setType(TypeConfig.fromString("TYPE"));
        assertEquals("TYPE", fieldConfig.getType().getType());
    }

    /** Should return null when the type is null */
    @Test
    public void getTypeWhenTypeIsNull() {
        FieldConfig fieldConfig = new FieldConfig();
        assertNull(fieldConfig.getType());
    }
}
