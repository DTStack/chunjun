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
import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TimestampColumnTest {

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asIntWhenDataIsNotNullThenThrowException() {
        TimestampColumn timestampColumn =
                new TimestampColumn(new Timestamp(System.currentTimeMillis()));
        assertThrows(CastException.class, timestampColumn::asInt);
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asSqlDateWhenDataIsNullThenReturnNull() {
        TimestampColumn timestampColumn = new TimestampColumn(null);
        assertNull(timestampColumn.asSqlDate());
    }

    @Test
    @DisplayName("Should return the sql date when the data is not null")
    public void asSqlDateWhenDataIsNotNullThenReturnTheSqlDate() {
        TimestampColumn timestampColumn =
                new TimestampColumn(new Timestamp(System.currentTimeMillis()));
        assertNotNull(timestampColumn.asSqlDate());
    }

    @Test
    @DisplayName("Should return the year of the timestamp")
    public void asYearIntShouldReturnTheYearOfTheTimestamp() {
        TimestampColumn timestampColumn =
                new TimestampColumn(Timestamp.valueOf("2020-01-01 00:00:00"));
        assertEquals(2020, timestampColumn.asYearInt());
    }

    @Test
    @DisplayName("Should return the precision of the timestamp")
    public void getPrecisionShouldReturnThePrecisionOfTheTimestamp() {
        TimestampColumn timestampColumn =
                new TimestampColumn(new Timestamp(System.currentTimeMillis()), 6);
        assertEquals(6, timestampColumn.getPrecision());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asTimeWhenDataIsNullThenReturnNull() {
        TimestampColumn timestampColumn = new TimestampColumn(null);
        assertNull(timestampColumn.asTime());
    }

    @Test
    @DisplayName("Should return a time when the data is not null")
    public void asTimeWhenDataIsNotNullThenReturnATime() {
        TimestampColumn timestampColumn =
                new TimestampColumn(new Timestamp(System.currentTimeMillis()));

        Time time = timestampColumn.asTime();

        assertNotNull(time);
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asShortWhenDataIsNotNullThenThrowException() {
        TimestampColumn timestampColumn =
                new TimestampColumn(new Timestamp(System.currentTimeMillis()));
        assertThrows(CastException.class, timestampColumn::asShort);
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asTimestampWhenDataIsNullThenReturnNull() {
        TimestampColumn timestampColumn = new TimestampColumn(null);
        assertNull(timestampColumn.asTimestamp());
    }

    @Test
    @DisplayName("Should return the timestamp when the data is not null")
    public void asTimestampWhenDataIsNotNullThenReturnTheTimestamp() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        TimestampColumn timestampColumn = new TimestampColumn(timestamp);
        assertEquals(timestamp, timestampColumn.asTimestamp());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asLongWhenDataIsNullThenReturnNull() {
        TimestampColumn timestampColumn = new TimestampColumn(null);
        assertNull(timestampColumn.asLong());
    }

    @Test
    @DisplayName("Should return the time in milliseconds when the data is not null")
    public void asLongWhenDataIsNotNullThenReturnTimeInMilliseconds() {
        TimestampColumn timestampColumn = new TimestampColumn(new Timestamp(1588291200000L));
        assertEquals(1588291200000L, timestampColumn.asLong());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asBigDecimalWhenDataIsNullThenReturnNull() {
        TimestampColumn timestampColumn = new TimestampColumn(null);
        assertNull(timestampColumn.asBigDecimal());
    }

    @Test
    @DisplayName("Should return a bigdecimal when the data is not null")
    public void asBigDecimalWhenDataIsNotNullThenReturnABigDecimal() {
        TimestampColumn timestampColumn =
                new TimestampColumn(new Timestamp(System.currentTimeMillis()));
        assertEquals(BigDecimal.class, timestampColumn.asBigDecimal().getClass());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asBytesWhenDataIsNullThenReturnNull() {
        TimestampColumn timestampColumn = new TimestampColumn(null);
        assertNull(timestampColumn.asBytes());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asBytesWhenDataIsNotNullThenThrowException() {
        TimestampColumn timestampColumn =
                new TimestampColumn(new Timestamp(System.currentTimeMillis()));
        assertThrows(CastException.class, timestampColumn::asBytes);
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asBooleanWhenDataIsNullThenReturnNull() {
        TimestampColumn timestampColumn = new TimestampColumn(null);
        assertNull(timestampColumn.asBoolean());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asBooleanWhenDataIsNotNullThenThrowException() {
        TimestampColumn timestampColumn =
                new TimestampColumn(new Timestamp(System.currentTimeMillis()));
        assertThrows(CastException.class, timestampColumn::asBoolean);
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asStringWhenDataIsNullThenReturnNull() {
        TimestampColumn timestampColumn = new TimestampColumn(null);
        assertNull(timestampColumn.asString());
    }
}
