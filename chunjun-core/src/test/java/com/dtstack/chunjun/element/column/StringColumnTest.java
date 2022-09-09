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

package com.dtstack.chunjun.element.column;

import com.dtstack.chunjun.throwable.CastException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StringColumnTest {

    @Test
    @DisplayName("Should set the format when the format is not blank")
    public void setFormatWhenFormatIsNotBlank() {
        StringColumn stringColumn = new StringColumn("test", "yyyy-MM-dd HH:mm:ss", false, 0);
        stringColumn.setFormat("yyyy-MM-dd");
        assertEquals("yyyy-MM-dd", stringColumn.getFormat());
    }

    @Test
    @DisplayName("Should return the format")
    public void getFormatShouldReturnTheFormat() {
        StringColumn stringColumn = new StringColumn("test", "yyyy-MM-dd HH:mm:ss", true, 0);
        assertEquals("yyyy-MM-dd HH:mm:ss", stringColumn.getFormat());
    }

    @Test
    @DisplayName("Should return true when the format is not blank")
    public void isCustomFormatWhenFormatIsNotBlankThenReturnTrue() {
        StringColumn stringColumn = new StringColumn("test", "yyyy-MM-dd HH:mm:ss", true, 0);
        assertTrue(stringColumn.isCustomFormat());
    }

    @Test
    @DisplayName("Should return false when the format is blank")
    public void isCustomFormatWhenFormatIsBlankThenReturnFalse() {
        StringColumn stringColumn = new StringColumn("test", "", false, 0);
        assertFalse(stringColumn.isCustomFormat());
    }

    @Test
    @DisplayName("Should return the sql date when the data is not null")
    public void asSqlDateWhenDataIsNotNullThenReturnTheSqlDate() {
        StringColumn stringColumn = new StringColumn("2020-01-01");
        java.sql.Date sqlDate = stringColumn.asSqlDate();
        assertEquals(sqlDate, java.sql.Date.valueOf("2020-01-01"));
    }

    @Test
    @DisplayName("Should return time when data is not null")
    public void asTimeWhenDataIsNotNullThenReturnTime() {
        String data = "2020-01-01 00:00:00";
        StringColumn stringColumn = new StringColumn(data);
        assertNotNull(stringColumn.asTime());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not a number")
    public void asBigDecimalWhenDataIsNotANumberThenThrowException() {
        StringColumn stringColumn = new StringColumn("abc");
        assertThrows(CastException.class, stringColumn::asBigDecimal);
    }

    @Test
    @DisplayName("Should return bytes when data is not null")
    public void asBytesWhenDataIsNotNullThenReturnBytes() {
        String data = "test";
        StringColumn stringColumn = new StringColumn(data);
        byte[] expected = data.getBytes(StandardCharsets.UTF_8);

        byte[] actual = stringColumn.asBytes();

        assertArrayEquals(expected, actual);
    }

    @Test
    @DisplayName("Should return true when the data is 1")
    public void asBooleanWhenDataIs1ThenReturnTrue() {
        StringColumn stringColumn = new StringColumn("1");
        assertTrue(stringColumn.asBoolean());
    }

    @Test
    @DisplayName("Should return false when the data is 0")
    public void asBooleanWhenDataIs0ThenReturnFalse() {
        StringColumn stringColumn = new StringColumn("0");
        assertFalse(stringColumn.asBoolean());
    }

    @Test
    @DisplayName("Should return date when data is a number")
    public void asDateWhenDataIsANumberThenReturnDate() {
        StringColumn stringColumn = new StringColumn("1577808000000");
        Date date = stringColumn.asDate();
        assertNotNull(date);
    }

    @Test
    @DisplayName("Should return a stringcolumn when the data is not null")
    public void fromWhenDataIsNotNullThenReturnStringColumn() {
        String data = "test";
        String format = "yyyy-MM-dd HH:mm:ss";
        boolean isCustomFormat = true;
        StringColumn stringColumn = StringColumn.from(data, format, isCustomFormat);
        assertTrue(stringColumn instanceof StringColumn);
    }

    @Test
    @DisplayName("Should return a stringcolumn when the format is not null")
    public void fromWhenFormatIsNotNullThenReturnStringColumn() {
        StringColumn stringColumn = StringColumn.from("test", "yyyy-MM-dd HH:mm:ss", true);
        assertTrue(stringColumn instanceof StringColumn);
    }

    @Test
    @DisplayName("Should return the string value of data when iscustomformat is false")
    public void asStringWhenIsCustomFormatIsFalseThenReturnTheStringValueOfData() {
        StringColumn stringColumn = new StringColumn("test");
        assertEquals("test", stringColumn.asString());
    }
}
