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

import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TimeColumnTest {

    @Test
    @DisplayName("Should return null when the data is null")
    public void asTimestampStrWhenDataIsNullThenReturnNull() {
        TimeColumn timeColumn = new TimeColumn(null);
        assertNull(timeColumn.asTimestampStr());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asSqlDateWhenDataIsNullThenReturnNull() {
        TimeColumn timeColumn = new TimeColumn(null);
        assertNull(timeColumn.asSqlDate());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asSqlDateWhenDataIsNotNullThenThrowException() {
        Time time = new Time(System.currentTimeMillis());
        TimeColumn timeColumn = new TimeColumn(time);
        assertThrows(CastException.class, () -> timeColumn.asSqlDate());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asTimeWhenDataIsNullThenReturnNull() {
        TimeColumn timeColumn = new TimeColumn(null);
        assertNull(timeColumn.asTime());
    }

    @Test
    @DisplayName("Should return the data when the data is not null")
    public void asTimeWhenDataIsNotNullThenReturnTheData() {
        Time data = Time.valueOf(LocalTime.ofNanoOfDay(1_000_000L));
        TimeColumn timeColumn = new TimeColumn(data);
        assertEquals(data, timeColumn.asTime());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asShortWhenDataIsNotNullThenThrowException() {
        TimeColumn timeColumn = new TimeColumn(Time.valueOf(LocalTime.ofNanoOfDay(1_000_000L)));
        assertThrows(CastException.class, timeColumn::asShort);
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asIntWhenDataIsNotNullThenThrowException() {
        TimeColumn timeColumn = new TimeColumn(Time.valueOf("00:00:00"));
        assertThrows(CastException.class, timeColumn::asInt);
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asBigDecimalWhenDataIsNullThenReturnNull() {
        TimeColumn timeColumn = new TimeColumn(null);
        assertNull(timeColumn.asBigDecimal());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asTimestampWhenDataIsNullThenReturnNull() {
        TimeColumn timeColumn = new TimeColumn(null);
        assertNull(timeColumn.asTimestamp());
    }

    @Test
    @DisplayName("Should return a timestamp when the data is not null")
    public void asTimestampWhenDataIsNotNullThenReturnATimestamp() {
        Time time = Time.valueOf(LocalTime.ofNanoOfDay(1_000_000L));
        TimeColumn timeColumn = new TimeColumn(time);
        Timestamp timestamp = timeColumn.asTimestamp();
        assertEquals(time.getTime(), timestamp.getTime());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asStringWhenDataIsNullThenReturnNull() {
        TimeColumn timeColumn = new TimeColumn(null);
        assertNull(timeColumn.asString());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asBooleanWhenDataIsNullThenReturnNull() {
        TimeColumn timeColumn = new TimeColumn(null);
        assertNull(timeColumn.asBoolean());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asBooleanWhenDataIsNotNullThenThrowException() {
        TimeColumn timeColumn = new TimeColumn(Time.valueOf(LocalTime.ofNanoOfDay(1_000_000L)));
        assertThrows(CastException.class, timeColumn::asBoolean);
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asBytesWhenDataIsNullThenReturnNull() {
        TimeColumn timeColumn = new TimeColumn(null);
        assertNull(timeColumn.asBytes());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asBytesWhenDataIsNotNullThenThrowException() {
        TimeColumn timeColumn = new TimeColumn(Time.valueOf("12:00:00"));
        assertThrows(CastException.class, () -> timeColumn.asBytes());
    }
}
