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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MapColumnTest {

    @Test
    @DisplayName("Should return null when data is null")
    public void asSqlDateWhenDataIsNullThenReturnNull() {
        MapColumn mapColumn = new MapColumn(null);
        assertNull(mapColumn.asSqlDate());
    }

    @Test
    @DisplayName("Should throw an exception when data is not null")
    public void asSqlDateWhenDataIsNotNullThenThrowException() {
        Map<String, Object> data = new HashMap<>();
        data.put("key", "value");
        MapColumn mapColumn = new MapColumn(data);

        CastException exception = assertThrows(CastException.class, mapColumn::asSqlDate);

        assertEquals(
                "Map[{\"key\":\"value\"}] can not cast to java.sql.Date.", exception.getMessage());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asTimestampStrWhenDataIsNullThenReturnNull() {
        MapColumn mapColumn = new MapColumn(null);
        assertNull(mapColumn.asTimestampStr());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asTimestampStrWhenDataIsNotNullThenThrowException() {
        Map<String, Object> data = new HashMap<>();
        data.put("key", "value");
        MapColumn mapColumn = new MapColumn(data);
        assertThrows(CastException.class, mapColumn::asTimestampStr);
    }

    @Test
    @DisplayName("Should return null when data is null")
    public void asTimeWhenDataIsNullThenReturnNull() {
        MapColumn mapColumn = new MapColumn(null);
        assertNull(mapColumn.asTime());
    }

    @Test
    @DisplayName("Should throw an exception when data is not null")
    public void asTimeWhenDataIsNotNullThenThrowException() {
        Map<String, Object> data = new HashMap<>();
        data.put("key", "value");
        MapColumn mapColumn = new MapColumn(data);
        assertThrows(CastException.class, () -> mapColumn.asTime());
    }

    @Test
    @DisplayName("Should return null when data is null")
    public void asBigDecimalWhenDataIsNullThenReturnNull() {
        MapColumn mapColumn = new MapColumn(null);
        assertNull(mapColumn.asBigDecimal());
    }

    @Test
    @DisplayName("Should throw an exception when data is not null")
    public void asBigDecimalWhenDataIsNotNullThenThrowException() {
        Map<String, Object> data = new HashMap<>();
        data.put("key", "value");
        MapColumn mapColumn = new MapColumn(data);
        assertThrows(CastException.class, mapColumn::asBigDecimal);
    }

    @Test
    @DisplayName("Should return null when data is null")
    public void asStringWhenDataIsNullThenReturnNull() {
        MapColumn mapColumn = new MapColumn(null);
        assertNull(mapColumn.asString());
    }

    @Test
    @DisplayName("Should return json string when data is not null")
    public void asStringWhenDataIsNotNullThenReturnJsonString() {
        Map<String, Object> data = new HashMap<>();
        data.put("key1", "value1");
        data.put("key2", "value2");
        MapColumn mapColumn = new MapColumn(data);
        assertEquals("{\"key1\":\"value1\",\"key2\":\"value2\"}", mapColumn.asString());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asTimestampWhenDataIsNotNullThenThrowException() {
        Map<String, Object> data = new HashMap<>();
        data.put("key", "value");
        MapColumn mapColumn = new MapColumn(data);
        assertThrows(CastException.class, mapColumn::asTimestamp);
    }

    @Test
    @DisplayName("Should return null when data is null")
    public void asBytesWhenDataIsNullThenReturnNull() {
        MapColumn mapColumn = new MapColumn(null);
        assertNull(mapColumn.asBytes());
    }

    @Test
    @DisplayName("Should return null when data is null")
    public void asBooleanWhenDataIsNullThenReturnNull() {
        MapColumn mapColumn = new MapColumn(null);
        assertNull(mapColumn.asBoolean());
    }

    @Test
    @DisplayName("Should throw an exception when data is not null")
    public void asBooleanWhenDataIsNotNullThenThrowException() {
        Map<String, Object> data = new HashMap<>();
        data.put("key", "value");
        MapColumn mapColumn = new MapColumn(data);
        assertThrows(CastException.class, mapColumn::asBoolean);
    }

    @Test
    @DisplayName("Should return null when data is null")
    public void asDateWhenDataIsNullThenReturnNull() {
        MapColumn mapColumn = new MapColumn(null);
        assertNull(mapColumn.asDate());
    }

    @Test
    @DisplayName("Should throw an exception when data is not null")
    public void asDateWhenDataIsNotNullThenThrowException() {
        Map<String, Object> data = new HashMap<>();
        data.put("key", "value");
        MapColumn mapColumn = new MapColumn(data);
        assertThrows(CastException.class, mapColumn::asDate);
    }
}
