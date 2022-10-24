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

import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class ErrorMsgRowDataTest {

    @Test
    @DisplayName("Should return null when the position is not 0")
    public void getStringWhenPositionIsNotZeroThenReturnNull() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");
        StringData result = errorMsgRowData.getString(1);
        assertNull(result);
    }

    @Test
    @DisplayName("Should return null when the pos is not 0")
    public void getRawValueWhenPosIsNot0ThenReturnNull() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");

        RawValueData<String> rawValueData = errorMsgRowData.getRawValue(1);

        assertNull(rawValueData);
    }

    @Test
    @DisplayName("Should return null when the pos is not 0")
    public void getTimestampWhenPosIsNot0ThenReturnNull() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");

        TimestampData timestamp = errorMsgRowData.getTimestamp(1, 0);

        assertNull(timestamp);
    }

    @Test
    @DisplayName("Should return null when the pos is not 0")
    public void getMapWhenPosIsNotZeroThenReturnNull() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");
        assertNull(errorMsgRowData.getMap(1));
    }

    @Test
    @DisplayName("Should return null when the pos is 0")
    public void getMapWhenPosIsZeroThenReturnNull() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");

        MapData map = errorMsgRowData.getMap(0);

        assertNull(map);
    }

    @Test
    @DisplayName("Should return the error message")
    public void toStringShouldReturnErrorMsg() {
        String errorMsg = "Error message";
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData(errorMsg);

        assertEquals(errorMsg, errorMsgRowData.toString());
    }

    @Test
    @DisplayName("Should return an empty byte array")
    public void getBinaryShouldReturnEmptyByteArray() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");
        byte[] expected = new byte[0];
        byte[] actual = errorMsgRowData.getBinary(0);
        assertArrayEquals(expected, actual);
    }

    @Test
    @DisplayName("Should return 0 when the position is not valid")
    public void getDoubleWhenPositionIsNotValidThenReturnZero() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");
        assertEquals(0, errorMsgRowData.getDouble(0));
    }

    @Test
    @DisplayName("Should return 0 when the position is not 0")
    public void getLongWhenPositionIsNotZeroThenReturnZero() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");
        assertEquals(0, errorMsgRowData.getLong(1));
    }

    @Test
    @DisplayName("Should return 0 when the position is 0")
    public void getLongWhenPositionIsZeroThenReturnZero() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");

        long result = errorMsgRowData.getLong(0);

        assertEquals(0, result);
    }

    @Test
    @DisplayName("Should return 0 when the position is 0")
    public void getIntWhenPositionIs0ThenReturn0() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");
        assertEquals(0, errorMsgRowData.getInt(0));
    }

    @Test
    @DisplayName("Should return 0 when the position is 1")
    public void getIntWhenPositionIs1ThenReturn0() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");

        int result = errorMsgRowData.getInt(1);

        assertEquals(0, result);
    }

    @Test
    @DisplayName("Should return 0 when the position is not valid")
    public void getFloatWhenPositionIsNotValidThenReturnZero() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("Error");
        assertEquals(0, errorMsgRowData.getFloat(0));
    }

    @Test
    @DisplayName("Should return 0 when the position is 0")
    public void getByteWhenPositionIs0ThenReturn0() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");
        assertEquals(0, errorMsgRowData.getByte(0));
    }

    @Test
    @DisplayName("Should return 0 when the position is 1")
    public void getByteWhenPositionIs1ThenReturn0() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");

        byte result = errorMsgRowData.getByte(1);

        assertEquals(0, result);
    }

    @Test
    @DisplayName("Should return false when the value is null")
    public void getBooleanWhenValueIsNullThenReturnFalse() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");

        boolean result = errorMsgRowData.getBoolean(0);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should return false when the pos is 0")
    public void isNullAtWhenPosIs0ThenReturnFalse() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");

        boolean result = errorMsgRowData.isNullAt(0);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should return insert")
    public void getRowKindShouldReturnInsert() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");
        assertEquals(RowKind.INSERT, errorMsgRowData.getRowKind());
    }

    @Test
    @DisplayName("Should return 0")
    public void getArityShouldReturn0() {
        ErrorMsgRowData errorMsgRowData = new ErrorMsgRowData("error");
        assertEquals(0, errorMsgRowData.getArity());
    }
}
