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

import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FieldConfTest {

    @Test
    @DisplayName("Should return an empty list when the fieldlist is empty")
    public void getFieldListWhenFieldListIsEmptyThenReturnEmptyList() {
        List<FieldConf> fieldList = FieldConf.getFieldList(Collections.emptyList());
        assertNotNull(fieldList);
        assertTrue(fieldList.isEmpty());
    }

    /** Should return the correct string when all fields are not null */
    @Test
    public void toStringWhenAllFieldsAreNotNull() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setName("name");
        fieldConf.setType("type");
        fieldConf.setIndex(1);
        fieldConf.setValue("value");
        fieldConf.setFormat("format");
        fieldConf.setParseFormat("parseFormat");
        fieldConf.setSplitter("splitter");
        fieldConf.setPart(true);
        fieldConf.setNotNull(true);
        fieldConf.setLength(1);

        String expected =
                "FieldConf{"
                        + "name='"
                        + "name"
                        + '\''
                        + ", type='"
                        + "type"
                        + '\''
                        + ", index="
                        + 1
                        + ", value='"
                        + "value"
                        + '\''
                        + ", format='"
                        + "format"
                        + '\''
                        + ", parseFormat='"
                        + "parseFormat"
                        + '\''
                        + ", splitter='"
                        + "splitter"
                        + '\''
                        + ", isPart="
                        + true
                        + ", notNull="
                        + true
                        + ", length="
                        + 1
                        + '}';

        assertEquals(expected, fieldConf.toString());
    }

    /** Should return a FieldConf object when the map is not empty */
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

        FieldConf fieldConf = FieldConf.getField(map, 1);

        assertEquals(fieldConf.getName(), "name");
        assertEquals(fieldConf.getType(), "type");
        assertEquals(fieldConf.getIndex(), Integer.valueOf(1));
        assertEquals(fieldConf.getValue(), "value");
        assertEquals(fieldConf.getFormat(), "format");
        assertEquals(fieldConf.getParseFormat(), "parseFormat");
        assertEquals(fieldConf.getSplitter(), "splitter");
        assertTrue(fieldConf.getPart());
        assertTrue(fieldConf.getNotNull());
        assertEquals(fieldConf.getLength(), Integer.valueOf(1));
    }

    /** Should return a FieldConf object when the map is empty */
    @Test
    public void getFieldWhenMapIsEmptyThenReturnFieldConfObject() {
        Map<String, Object> map = new HashMap<>();
        FieldConf fieldConf = FieldConf.getField(map, 0);
        assertNotNull(fieldConf);
    }

    /** Should return the parseFormat when the parseFormat is not null */
    @Test
    public void getParseFormatWhenParseFormatIsNotNull() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setParseFormat("yyyy-MM-dd HH:mm:ss");
        assertEquals("yyyy-MM-dd HH:mm:ss", fieldConf.getParseFormat());
    }

    /** Should return null when the parseFormat is null */
    @Test
    public void getParseFormatWhenParseFormatIsNull() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setParseFormat(null);
        assertNull(fieldConf.getParseFormat());
    }

    /** Should return the value when the value is not null */
    @Test
    public void getValueWhenValueIsNotNull() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setValue("value");
        assertEquals("value", fieldConf.getValue());
    }

    /** Should return null when the value is null */
    @Test
    public void getValueWhenValueIsNull() {
        FieldConf fieldConf = new FieldConf();
        assertNull(fieldConf.getValue());
    }

    /** Should return the format when the format is not null */
    @Test
    public void getFormatWhenFormatIsNotNull() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setFormat("yyyy-MM-dd HH:mm:ss");
        assertEquals("yyyy-MM-dd HH:mm:ss", fieldConf.getFormat());
    }

    /** Should return null when the format is null */
    @Test
    public void getFormatWhenFormatIsNull() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setFormat(null);
        assertNull(fieldConf.getFormat());
    }

    /** Should return the splitter when the splitter is not null */
    @Test
    public void getSplitterWhenSplitterIsNotNull() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setSplitter("splitter");
        assertEquals("splitter", fieldConf.getSplitter());
    }

    /** Should return null when the splitter is null */
    @Test
    public void getSplitterWhenSplitterIsNull() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setSplitter(null);
        assertNull(fieldConf.getSplitter());
    }

    /** Should return true when the notNull is true */
    @Test
    public void getNotNullWhenNotNullIsTrue() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setNotNull(true);
        assertTrue(fieldConf.getNotNull());
    }

    /** Should return false when the notNull is false */
    @Test
    public void getNotNullWhenNotNullIsFalse() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setNotNull(false);
        assertFalse(fieldConf.getNotNull());
    }

    /** Should return the length of the field */
    @Test
    public void getLengthShouldReturnTheLengthOfTheField() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setLength(10);
        assertEquals(10, (long) fieldConf.getLength());
    }

    /** Should return true when the field is part */
    @Test
    public void getPartWhenFieldIsPart() {
        FieldConf field = new FieldConf();
        field.setPart(true);
        assertTrue(field.getPart());
    }

    /** Should return false when the field is not part */
    @Test
    public void getPartWhenFieldIsNotPart() {
        FieldConf field = new FieldConf();
        field.setPart(false);
        assertFalse(field.getPart());
    }

    /** Should return null when the fieldList is empty */
    @Test
    public void getSameNameMetaColumnWhenFieldListIsEmptyThenReturnNull() {
        List<FieldConf> fieldList = new ArrayList<>();
        FieldConf fieldConf = FieldConf.getSameNameMetaColumn(fieldList, "name");
        assertNull(fieldConf);
    }

    /**
     * Should return null when the fieldList is not empty but there is no field with the same name
     */
    @Test
    public void
            getSameNameMetaColumnWhenFieldListIsNotEmptyButThereIsNoFieldWithTheSameNameThenReturnNull() {
        List<FieldConf> fieldList = new ArrayList<>();
        FieldConf field1 = new FieldConf();
        field1.setName("name1");
        fieldList.add(field1);
        FieldConf field2 = new FieldConf();
        field2.setName("name2");
        fieldList.add(field2);
        FieldConf field3 = new FieldConf();
        field3.setName("name3");
        fieldList.add(field3);

        String name = "name4";

        FieldConf result = FieldConf.getSameNameMetaColumn(fieldList, name);

        assertNull(result);
    }

    /** Should return the index when the index is not null */
    @Test
    public void getIndexWhenIndexIsNotNull() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setIndex(1);
        assertEquals(1, fieldConf.getIndex().intValue());
    }

    /** Should return the name of the field */
    @Test
    public void getNameShouldReturnTheNameOfTheField() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setName("name");
        assertEquals("name", fieldConf.getName());
    }

    /** Should return the type when the type is not null */
    @Test
    public void getTypeWhenTypeIsNotNull() {
        FieldConf fieldConf = new FieldConf();
        fieldConf.setType("type");
        assertEquals("type", fieldConf.getType());
    }

    /** Should return null when the type is null */
    @Test
    public void getTypeWhenTypeIsNull() {
        FieldConf fieldConf = new FieldConf();
        assertNull(fieldConf.getType());
    }
}
