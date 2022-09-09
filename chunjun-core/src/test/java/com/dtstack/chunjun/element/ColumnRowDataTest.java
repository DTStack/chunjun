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

package com.dtstack.chunjun.element;

import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.StringColumn;

import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ColumnRowDataTest {

    @Test
    @DisplayName("Should return the string of the row data")
    public void getStringShouldReturnTheStringOfTheRowData() {
        ColumnRowData columnRowData = new ColumnRowData(3);
        columnRowData.addHeader("name");
        columnRowData.addHeader("age");
        columnRowData.addHeader("gender");
        columnRowData.addField(new StringColumn("John"));
        columnRowData.addField(new StringColumn("male"));

        String result = columnRowData.getString();

        assertEquals("(John,male)", result);
    }

    @Test
    @DisplayName("Should return null when the pos is not in the range")
    public void getRowWhenPosIsNotInRangeThenReturnNull() {
        ColumnRowData columnRowData = new ColumnRowData(2);
        columnRowData.addHeader("name");
        columnRowData.addHeader("age");
        columnRowData.addField(new StringColumn("zhangsan"));

        assertNull(columnRowData.getRow(-1, 2));
        assertNull(columnRowData.getRow(2, 2));
    }

    @Test
    @DisplayName("Should return null when the numfields is not in the range")
    public void getRowWhenNumFieldsIsNotInRangeThenReturnNull() {
        ColumnRowData columnRowData = new ColumnRowData(2);
        columnRowData.addHeader("name");
        columnRowData.addHeader("age");
        columnRowData.addField(new StringColumn("John"));

        assertNull(columnRowData.getRow(0, 3));
    }

    @Test
    @DisplayName("Should return null when the pos is not in the range")
    public void getMapWhenPosIsNotInRangeThenReturnNull() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addHeader("name");
        columnRowData.addField(new StringColumn("test"));

        MapData mapData = columnRowData.getMap(1);

        assertNull(mapData);
    }

    @Test
    @DisplayName("Should return null when the pos is in the range")
    public void getMapWhenPosIsInRangeThenReturnNull() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addHeader("name");
        columnRowData.addField(new StringColumn("value"));

        MapData mapData = columnRowData.getMap(0);

        assertNull(mapData);
    }

    @Test
    @DisplayName("Should return null when the pos is not in the range")
    public void getRawValueWhenPosIsNotInRangeThenReturnNull() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        assertNull(columnRowData.getRawValue(1));
    }

    @Test
    @DisplayName("Should return null when the pos is in the range")
    public void getRawValueWhenPosIsInRangeThenReturnNull() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addField(new StringColumn("test"));

        RawValueData<String> rawValueData = columnRowData.getRawValue(0);

        assertNull(rawValueData);
    }

    @Test
    @DisplayName("Should return the byte value of the column")
    public void getByteShouldReturnTheByteValueOfTheColumn() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addHeader("header");
        columnRowData.addField(new ByteColumn((byte) 1));

        byte result = columnRowData.getByte(0);

        assertEquals((byte) 1, result);
    }

    @Test
    @DisplayName("Should return true when the column is null")
    public void isNullAtWhenColumnIsNull() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addField(null);
        assertTrue(columnRowData.isNullAt(0));
    }

    @Test
    @DisplayName("Should return true when the column is boolean and the value is true")
    public void getBooleanWhenColumnIsBooleanAndValueIsTrue() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addField(new BooleanColumn(true));

        Boolean result = columnRowData.getBoolean(0);

        assertTrue(result);
    }

    @Test
    @DisplayName("Should return false when the column is boolean and the value is false")
    public void getBooleanWhenColumnIsBooleanAndValueIsFalse() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addHeader("boolean");
        columnRowData.addField(new BooleanColumn(false));

        boolean result = columnRowData.getBoolean(0);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should throw an exception when the kind is null")
    public void setRowKindWhenKindIsNullThenThrowException() {
        ColumnRowData columnRowData = new ColumnRowData(0);
        assertThrows(NullPointerException.class, () -> columnRowData.setRowKind(null));
    }

    @Test
    @DisplayName("Should return a new columnrowdata with the same extheader")
    public void copyShouldReturnNewColumnRowDataWithSameExtHeader() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addExtHeader("extheader");
        ColumnRowData copy = columnRowData.copy();
        assertEquals(columnRowData.getExtHeader(), copy.getExtHeader());
    }

    @Test
    @DisplayName(
            "Should return the column when the header is not null and the header contains the name")
    public void getFieldWhenHeaderIsNotNullAndHeaderContainsNameThenReturnColumn() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addHeader("name");
        columnRowData.addField(new StringColumn("value"));

        AbstractBaseColumn column = columnRowData.getField("name");

        assertNotNull(column);
    }

    @Test
    @DisplayName(
            "Should return null when the header is not null and the header does not contain the name")
    public void getFieldWhenHeaderIsNotNullAndHeaderDoesNotContainNameThenReturnNull() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addHeader("name");
        assertNull(columnRowData.getField("name1"));
    }

    @Test
    @DisplayName("Should add all fields to columnlist")
    public void addAllFieldShouldAddAllFieldsToColumnList() {
        ColumnRowData columnRowData = new ColumnRowData(2);
        List<AbstractBaseColumn> list = new ArrayList<>();
        list.add(new StringColumn("test"));
        list.add(new StringColumn("test2"));

        columnRowData.addAllField(list);

        assertEquals(2, columnRowData.getArity());
    }

    @Test
    @DisplayName("Should add all headers to the header map")
    public void addAllHeaderShouldAddAllHeadersToTheHeaderMap() {
        List<String> headers = Arrays.asList("header1", "header2", "header3");
        ColumnRowData columnRowData = new ColumnRowData(headers.size());

        columnRowData.addAllHeader(headers);

        assertEquals(headers.size(), columnRowData.getHeaderInfo().size());
        assertTrue(columnRowData.getHeaderInfo().containsKey("header1"));
        assertTrue(columnRowData.getHeaderInfo().containsKey("header2"));
        assertTrue(columnRowData.getHeaderInfo().containsKey("header3"));
    }

    @Test
    @DisplayName("Should add the column to the columnlist")
    public void addFieldWithOutByteSizeShouldAddColumnToColumnList() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        AbstractBaseColumn column = new StringColumn("test");
        columnRowData.addFieldWithOutByteSize(column);
        assertEquals(column, columnRowData.getField(0));
    }

    @Test
    @DisplayName("Should return null when header is null")
    public void getHeadersWhenHeaderIsNullThenReturnNull() {
        ColumnRowData columnRowData = new ColumnRowData(0);
        assertNull(columnRowData.getHeaders());
    }

    @Test
    @DisplayName("Should return the headers in order when header is not null")
    public void getHeadersWhenHeaderIsNotNullThenReturnTheHeadersInOrder() {
        ColumnRowData columnRowData = new ColumnRowData(3);
        columnRowData.addHeader("name");
        columnRowData.addHeader("age");
        columnRowData.addHeader("gender");

        String[] headers = columnRowData.getHeaders();

        assertArrayEquals(new String[] {"name", "age", "gender"}, headers);
    }

    @Test
    @DisplayName("Should add the extheader when the extheader is not in the extheader set")
    public void addExtHeaderWhenExtHeaderIsNotInTheExtHeaderSet() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        String extHeader = "extHeader";
        columnRowData.addExtHeader(extHeader);
        assertTrue(columnRowData.isExtHeader(extHeader));
    }

    @Test
    @DisplayName("Should return true when the name is in extheader")
    public void isExtHeaderWhenNameIsInExtHeaderThenReturnTrue() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addExtHeader("name");
        assertTrue(columnRowData.isExtHeader("name"));
    }

    @Test
    @DisplayName("Should return false when the name is not in extheader")
    public void isExtHeaderWhenNameIsNotInExtHeaderThenReturnFalse() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addExtHeader("name");
        assertFalse(columnRowData.isExtHeader("name1"));
    }

    @Test
    @DisplayName("Should return the extheader")
    public void getExtHeaderShouldReturnTheExtHeader() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addExtHeader("extheader");
        assertEquals(columnRowData.getExtHeader().size(), 1);
    }

    @Test
    @DisplayName("Should replace the header when the original header is exist")
    public void replaceHeaderWhenOriginalHeaderIsExistThenReplaceTheHeader() {
        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addHeader("original");
        columnRowData.addField(new StringColumn("value"));

        columnRowData.replaceHeader("original", "another");

        assertEquals(1, columnRowData.getArity());
        assertEquals("another", columnRowData.getHeaders()[0]);
    }
}
