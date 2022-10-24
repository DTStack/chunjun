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

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BooleanColumnTest {
    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asTimeWhenDataIsNotNullThenThrowException() {
        BooleanColumn booleanColumn = new BooleanColumn(true);

        CastException exception = assertThrows(CastException.class, () -> booleanColumn.asTime());

        assertEquals("boolean[true] can not cast to java.sql.Time.", exception.getMessage());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asSqlDateWhenDataIsNotNullThenThrowException() {
        BooleanColumn booleanColumn = new BooleanColumn(true);

        CastException exception =
                assertThrows(CastException.class, () -> booleanColumn.asSqlDate());

        assertEquals("boolean[true] can not cast to java.sql.Date.", exception.getMessage());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asTimestampStrWhenDataIsNotNullThenThrowException() {
        BooleanColumn booleanColumn = new BooleanColumn(true);
        assertThrows(CastException.class, () -> booleanColumn.asTimestampStr());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asTimestampWhenDataIsNotNullThenThrowException() {
        BooleanColumn booleanColumn = new BooleanColumn(true);
        assertThrows(CastException.class, () -> booleanColumn.asTimestamp());
    }

    @Test
    @DisplayName("Should return true when the data is true")
    public void asStringWhenDataIsTrueThenReturnTrue() {
        BooleanColumn booleanColumn = new BooleanColumn(true);
        assertEquals("true", booleanColumn.asString());
    }

    @Test
    @DisplayName("Should return 1 when the data is true")
    public void asBigDecimalWhenDataIsTrueThenReturn1() {
        BooleanColumn booleanColumn = new BooleanColumn(true);
        BigDecimal result = booleanColumn.asBigDecimal();
        assertEquals(BigDecimal.valueOf(1L), result);
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asBytesWhenDataIsNotNullThenThrowException() {
        BooleanColumn booleanColumn = new BooleanColumn(true);
        assertThrows(CastException.class, () -> booleanColumn.asBytes());
    }

    @Test
    @DisplayName("Should return true when the data is true")
    public void asBooleanWhenDataIsTrueThenReturnTrue() {
        BooleanColumn booleanColumn = new BooleanColumn(true);
        assertTrue(booleanColumn.asBoolean());
    }

    @Test
    @DisplayName("Should return a booleancolumn when the data is true")
    public void fromWhenDataIsTrue() {
        BooleanColumn booleanColumn = BooleanColumn.from(true);
        assertEquals(true, booleanColumn.asBoolean());
    }

    @Test
    @DisplayName("Should return a booleancolumn when the data is false")
    public void fromWhenDataIsFalse() {
        BooleanColumn booleanColumn = BooleanColumn.from(false);
        assertEquals(false, booleanColumn.asBoolean());
    }
}
