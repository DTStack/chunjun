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
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlDateColumnTest {

    @Test
    @DisplayName("Should return the year of the date")
    public void asYearIntShouldReturnTheYearOfTheDate() {
        SqlDateColumn sqlDateColumn = new SqlDateColumn(Date.valueOf(LocalDate.of(2020, 1, 1)));
        assertEquals(2020, sqlDateColumn.asYearInt());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asTimestampStrWhenDataIsNullThenReturnNull() {
        SqlDateColumn sqlDateColumn = new SqlDateColumn(null);
        assertNull(sqlDateColumn.asTimestampStr());
    }

    @Test
    @DisplayName("Should return the string representation of the date when the data is not null")
    public void asTimestampStrWhenDataIsNotNullThenReturnStringRepresentationOfDate() {
        Date date = Date.valueOf("2020-01-01");
        SqlDateColumn sqlDateColumn = new SqlDateColumn(date);

        String result = sqlDateColumn.asTimestampStr();

        assertEquals("2020-01-01", result);
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asTimeWhenDataIsNullThenReturnNull() {
        SqlDateColumn sqlDateColumn = new SqlDateColumn(null);
        assertNull(sqlDateColumn.asTime());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asTimeWhenDataIsNotNullThenThrowException() {
        SqlDateColumn sqlDateColumn = new SqlDateColumn(Date.valueOf(LocalDate.ofEpochDay(1)));

        CastException exception = assertThrows(CastException.class, sqlDateColumn::asTime);

        assertEquals(
                "java.sql.Date[1970-01-02] can not cast to java.sql.Time.", exception.getMessage());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asSqlDateWhenDataIsNullThenReturnNull() {
        SqlDateColumn sqlDateColumn = new SqlDateColumn(null);
        assertNull(sqlDateColumn.asSqlDate());
    }

    @Test
    @DisplayName("Should return the date when the data is not null")
    public void asSqlDateWhenDataIsNotNullThenReturnTheDate() {
        Date date = new Date(System.currentTimeMillis());
        SqlDateColumn sqlDateColumn = new SqlDateColumn(date);

        Date result = sqlDateColumn.asSqlDate();

        assertEquals(date, result);
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asTimestampWhenDataIsNullThenReturnNull() {
        SqlDateColumn sqlDateColumn = new SqlDateColumn(null);
        assertNull(sqlDateColumn.asTimestamp());
    }

    @Test
    @DisplayName("Should return a timestamp when the data is not null")
    public void asTimestampWhenDataIsNotNullThenReturnATimestamp() {
        Date date = new Date(System.currentTimeMillis());
        SqlDateColumn sqlDateColumn = new SqlDateColumn(date);

        Timestamp timestamp = sqlDateColumn.asTimestamp();

        assertNotNull(timestamp);
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asBigDecimalWhenDataIsNullThenReturnNull() {
        SqlDateColumn sqlDateColumn = new SqlDateColumn(null);
        assertNull(sqlDateColumn.asBigDecimal());
    }

    @Test
    @DisplayName("Should return the epoch day when the data is not null")
    public void asBigDecimalWhenDataIsNotNullThenReturnEpochDay() {
        Date date = Date.valueOf("2020-01-01");
        SqlDateColumn sqlDateColumn = new SqlDateColumn(date);
        BigDecimal expected = BigDecimal.valueOf(date.toLocalDate().toEpochDay());
        assertEquals(expected, sqlDateColumn.asBigDecimal());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asStringWhenDataIsNullThenReturnNull() {
        SqlDateColumn sqlDateColumn = new SqlDateColumn(null);
        assertNull(sqlDateColumn.asString());
    }

    @Test
    @DisplayName("Should return the string representation of the date when the data is not null")
    public void asStringWhenDataIsNotNullThenReturnTheStringRepresentationOfTheDate() {
        Date date = Date.valueOf("2020-01-01");
        SqlDateColumn sqlDateColumn = new SqlDateColumn(date);

        String result = sqlDateColumn.asString();

        assertEquals("2020-01-01", result);
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asBytesWhenDataIsNullThenReturnNull() {
        SqlDateColumn sqlDateColumn = new SqlDateColumn(null);
        assertNull(sqlDateColumn.asBytes());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asBooleanWhenDataIsNullThenReturnNull() {
        SqlDateColumn sqlDateColumn = new SqlDateColumn(null);
        assertNull(sqlDateColumn.asBoolean());
    }

    @Test
    @DisplayName("Should throw an exception when the data is not null")
    public void asBooleanWhenDataIsNotNullThenThrowException() {
        SqlDateColumn sqlDateColumn = new SqlDateColumn(Date.valueOf(LocalDate.ofEpochDay(1)));

        CastException exception = assertThrows(CastException.class, sqlDateColumn::asBoolean);

        assertEquals("java.sql.Date[1970-01-02] can not cast to Boolean.", exception.getMessage());
    }
}
