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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ByteColumnTest {

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asSqlDateWhenDataIsNotNullThenThrowException() {
        ByteColumn byteColumn = new ByteColumn((byte) 1);
        assertThrows(CastException.class, byteColumn::asSqlDate);
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asTimeWhenDataIsNotNullThenThrowException() {
        ByteColumn byteColumn = new ByteColumn((byte) 1);
        assertThrows(CastException.class, byteColumn::asTime);
    }

    @Test
    @DisplayName("Should return a bigdecimal when the data is not null")
    public void asBigDecimalWhenDataIsNotNullThenReturnBigDecimal() {
        ByteColumn byteColumn = new ByteColumn((byte) 1);
        BigDecimal bigDecimal = byteColumn.asBigDecimal();
        assertEquals(new BigDecimal(1), bigDecimal);
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asTimestampWhenDataIsNotNullThenThrowException() {
        ByteColumn byteColumn = new ByteColumn((byte) 1);
        assertThrows(CastException.class, byteColumn::asTimestamp);
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asTimestampStrWhenDataIsNotNullThenThrowException() {
        ByteColumn byteColumn = new ByteColumn((byte) 1);
        assertThrows(CastException.class, byteColumn::asTimestampStr);
    }

    @Test
    @DisplayName("Should return the string representation of the byte")
    public void asStringShouldReturnTheStringRepresentationOfTheByte() {
        ByteColumn byteColumn = new ByteColumn((byte) 1);
        assertEquals("1", byteColumn.asString());
    }

    @Test
    @DisplayName("Should return a byte array with the same value as the data")
    public void asBytesShouldReturnByteArrayWithSameValueAsData() {
        ByteColumn byteColumn = new ByteColumn((byte) 0x01);
        assertArrayEquals(new byte[] {0x01}, byteColumn.asBytes());
    }

    @Test
    @DisplayName("Should return true when the data is not 0x00")
    public void asBooleanWhenDataIsNot0x00ThenReturnTrue() {
        ByteColumn byteColumn = new ByteColumn((byte) 0x01);
        assertTrue(byteColumn.asBoolean());
    }

    @Test
    @DisplayName("Should return false when the data is 0x00")
    public void asBooleanWhenDataIs0x00ThenReturnFalse() {
        ByteColumn byteColumn = new ByteColumn((byte) 0x00);
        assertFalse(byteColumn.asBoolean());
    }

    @Test
    @DisplayName("Should return a bytecolumn with the same data")
    public void fromShouldReturnByteColumnWithSameData() {
        byte data = 1;
        ByteColumn byteColumn = ByteColumn.from(data);
        assertEquals(data, byteColumn.getData());
    }

    @Test
    @DisplayName("Should return a bytecolumn with the same bytesize")
    public void fromShouldReturnByteColumnWithSameByteSize() {
        ByteColumn byteColumn = ByteColumn.from((byte) 0x01);
        assertEquals(0, byteColumn.getByteSize());
    }
}
