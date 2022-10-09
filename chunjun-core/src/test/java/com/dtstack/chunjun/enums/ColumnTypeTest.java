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

package com.dtstack.chunjun.enums;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ColumnTypeTest {

    @Test
    @DisplayName("Should return the correct type when the type is a time type")
    public void getTypeWhenTypeIsTimeTypeThenReturnCorrectType() {
        assertEquals(ColumnType.DATE, ColumnType.getType("date"));
        assertEquals(ColumnType.DATETIME, ColumnType.getType("datetime"));
        assertEquals(ColumnType.TIME, ColumnType.getType("time"));
        assertEquals(ColumnType.TIMESTAMP, ColumnType.getType("timestamp"));
    }

    @Test
    @DisplayName("Should return true when the type is string")
    public void isStringTypeWhenTypeIsStringThenReturnTrue() {
        assertTrue(ColumnType.isStringType("string"));
    }

    @Test
    @DisplayName("Should return false when the type is not string")
    public void isStringTypeWhenTypeIsNotStringThenReturnFalse() {
        assertFalse(ColumnType.isStringType("INT"));
    }

    @Test
    @DisplayName("Should return true when the type is int")
    public void isNumberTypeWhenTypeIsIntThenReturnTrue() {
        assertTrue(ColumnType.isNumberType("int"));
    }

    @Test
    @DisplayName("Should return true when the type is int2")
    public void isNumberTypeWhenTypeIsInt2ThenReturnTrue() {
        String type = "int2";
        boolean result = ColumnType.isNumberType(type);
        assertTrue(result);
    }

    @Test
    @DisplayName("Should return true when the type is date")
    public void isTimeTypeWhenTypeIsDateThenReturnTrue() {
        String type = "date";
        boolean result = ColumnType.isTimeType(type);
        assertTrue(result);
    }

    @Test
    @DisplayName("Should return true when the type is datetime")
    public void isTimeTypeWhenTypeIsDatetimeThenReturnTrue() {
        String type = "datetime";
        boolean result = ColumnType.isTimeType(type);
        assertTrue(result);
    }

    @Test
    @DisplayName("Should throw an exception when the type is null")
    public void fromStringWhenTypeIsNullThenThrowException() {
        assertThrows(RuntimeException.class, () -> ColumnType.fromString(null));
    }

    @Test
    @DisplayName("Should return string when the type is string")
    public void fromStringWhenTypeIsStringThenReturnSTRING() {
        assertEquals(ColumnType.STRING, ColumnType.fromString("string"));
    }
}
