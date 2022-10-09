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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BytesColumnTest {

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asTimeWhenDataIsNotNullThenThrowException() {
        byte[] data = "test".getBytes(Charset.forName(StandardCharsets.UTF_8.name()));
        BytesColumn bytesColumn = new BytesColumn(data);

        CastException exception = assertThrows(CastException.class, () -> bytesColumn.asTime());

        assertEquals("Bytes[test] can not cast to java.sql.Time.", exception.getMessage());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asTimestampWhenDataIsNotNullThenThrowException() {
        byte[] data = "test".getBytes(Charset.forName(StandardCharsets.UTF_8.name()));
        BytesColumn bytesColumn = new BytesColumn(data);

        CastException exception =
                assertThrows(CastException.class, () -> bytesColumn.asTimestamp());

        assertEquals("Bytes[test] can not cast to Timestamp.", exception.getMessage());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asBigDecimalWhenDataIsNotNullThenThrowException() {
        byte[] data = "test".getBytes(Charset.forName(StandardCharsets.UTF_8.name()));
        BytesColumn bytesColumn = new BytesColumn(data);

        CastException exception = assertThrows(CastException.class, bytesColumn::asBigDecimal);

        assertEquals("Bytes[test] can not cast to BigDecimal.", exception.getMessage());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asBooleanWhenDataIsNotNullThenThrowException() {
        byte[] data = "test".getBytes(Charset.forName(StandardCharsets.UTF_8.name()));
        BytesColumn bytesColumn = new BytesColumn(data);

        CastException exception = assertThrows(CastException.class, () -> bytesColumn.asBoolean());

        assertEquals("Bytes[test] can not cast to Boolean.", exception.getMessage());
    }

    @Test
    @DisplayName("Should return the data when the data is not null")
    public void asBytesWhenDataIsNotNullThenReturnTheData() {
        byte[] data = new byte[] {1, 2, 3};
        BytesColumn bytesColumn = new BytesColumn(data);

        byte[] result = bytesColumn.asBytes();

        assertArrayEquals(data, result);
    }

    @Test
    @DisplayName("Should return the string when the data is not null")
    public void asStringWhenDataIsNotNullThenReturnTheString() {
        byte[] data = "test".getBytes();
        BytesColumn bytesColumn = new BytesColumn(data);
        assertEquals("test", bytesColumn.asString());
    }

    @Test
    @DisplayName("Should return a bytescolumn when the data is not null")
    public void fromWhenDataIsNotNullThenReturnBytesColumn() {
        byte[] data = new byte[] {1, 2, 3};
        BytesColumn bytesColumn = BytesColumn.from(data);
        assertNotNull(bytesColumn);
    }

    @Test
    @DisplayName("Should return a bytescolumn when the data is null")
    public void fromWhenDataIsNullThenReturnBytesColumn() {
        byte[] data = null;

        BytesColumn bytesColumn = BytesColumn.from(data);

        assertNull(bytesColumn.getData());
    }
}
