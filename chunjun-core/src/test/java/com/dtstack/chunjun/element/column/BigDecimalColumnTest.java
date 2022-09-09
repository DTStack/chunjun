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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BigDecimalColumnTest {

    @Test
    @DisplayName("Should return the timestamp string when the data is not null")
    public void asTimestampStrWhenDataIsNotNullThenReturnTheTimestampString() {
        BigDecimalColumn bigDecimalColumn = new BigDecimalColumn(new BigDecimal(1));
        assertEquals("1970-01-01 08:00:00.001", bigDecimalColumn.asTimestampStr());
    }

    @Test
    @DisplayName("Should return a time when the data is not null")
    public void asTimeWhenDataIsNotNullThenReturnATime() {
        BigDecimalColumn bigDecimalColumn = new BigDecimalColumn(new BigDecimal(1));
        Time expected = new Time(1);
        assertEquals(expected, bigDecimalColumn.asTime());
    }

    @Test
    @DisplayName("Should return a timestamp when the data is not null")
    public void asTimestampWhenDataIsNotNullThenReturnATimestamp() {
        BigDecimalColumn bigDecimalColumn = new BigDecimalColumn(new BigDecimal(1));
        Timestamp expected = new Timestamp(1);

        Timestamp actual = bigDecimalColumn.asTimestamp();

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should return the sql date when the data is not null")
    public void asSqlDateWhenDataIsNotNullThenReturnTheSqlDate() {
        BigDecimalColumn bigDecimalColumn = new BigDecimalColumn(new BigDecimal(1));
        assertEquals("1970-01-01", bigDecimalColumn.asSqlDate().toString());
    }

    @Test
    @DisplayName("Should return the data when the data is not null")
    public void asBigDecimalWhenDataIsNotNullThenReturnTheData() {
        BigDecimalColumn bigDecimalColumn = new BigDecimalColumn(new BigDecimal(1));
        assertEquals(new BigDecimal(1), bigDecimalColumn.asBigDecimal());
    }

    @Test
    @DisplayName("Should return a date when the data is not null")
    public void asDateWhenDataIsNotNullThenReturnADate() {
        BigDecimalColumn bigDecimalColumn = new BigDecimalColumn(new BigDecimal(1));
        Date date = bigDecimalColumn.asDate();
        assertNotNull(date);
    }

    @Test
    @DisplayName("Should return true when the data is not zero")
    public void asBooleanWhenDataIsNotZeroThenReturnTrue() {
        BigDecimalColumn bigDecimalColumn = new BigDecimalColumn(new BigDecimal(1));
        assertTrue(bigDecimalColumn.asBoolean());
    }

    @Test
    @DisplayName("Should return false when the data is zero")
    public void asBooleanWhenDataIsZeroThenReturnFalse() {
        BigDecimalColumn bigDecimalColumn = new BigDecimalColumn(BigDecimal.ZERO);
        assertFalse(bigDecimalColumn.asBoolean());
    }

    @Test
    @DisplayName("Should return the string representation of the data when the data is not null")
    public void asStringWhenDataIsNotNullThenReturnTheStringRepresentationOfTheData() {
        BigDecimalColumn bigDecimalColumn = new BigDecimalColumn(new BigDecimal(1));
        assertEquals("1", bigDecimalColumn.asString());
    }

    @Test
    @DisplayName("Should return a bigdecimalcolumn when the data is not null")
    public void fromWhenDataIsNotNullThenReturnBigDecimalColumn() {
        BigDecimalColumn bigDecimalColumn = BigDecimalColumn.from(new BigDecimal(1));
        assertNotNull(bigDecimalColumn);
    }

    @Test
    @DisplayName("Should return a bigdecimalcolumn when the data is null")
    public void fromWhenDataIsNullThenReturnBigDecimalColumn() {
        BigDecimalColumn bigDecimalColumn = BigDecimalColumn.from(null);
        assertNull(bigDecimalColumn.getData());
    }
}
