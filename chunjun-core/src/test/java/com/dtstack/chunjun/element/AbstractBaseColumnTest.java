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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class AbstractBaseColumnTest {

    private AbstractBaseColumn abstractBaseColumn;

    @BeforeEach
    public void setUp() {
        abstractBaseColumn =
                new AbstractBaseColumn(null, 0) {
                    @Override
                    public String type() {
                        return null;
                    }

                    @Override
                    public Boolean asBoolean() {
                        return null;
                    }

                    @Override
                    public byte[] asBytes() {
                        return new byte[0];
                    }

                    @Override
                    public String asString() {
                        return null;
                    }

                    @Override
                    public BigDecimal asBigDecimal() {
                        return null;
                    }

                    @Override
                    public Timestamp asTimestamp() {
                        return null;
                    }

                    @Override
                    public Time asTime() {
                        return null;
                    }

                    @Override
                    public java.sql.Date asSqlDate() {
                        return null;
                    }

                    @Override
                    public String asTimestampStr() {
                        return null;
                    }
                };
    }

    @Test
    @DisplayName("Should return empty string when data is null")
    public void toStringWhenDataIsNullThenReturnEmptyString() {
        assertEquals("", abstractBaseColumn.toString());
    }

    @Test
    @DisplayName("Should set the data when the data is null")
    public void setDataWhenDataIsNull() {
        abstractBaseColumn.setData(null);
        assertNull(abstractBaseColumn.getData());
    }

    @Test
    @DisplayName("Should set the data when the data is not null")
    public void setDataWhenDataIsNotNull() {
        abstractBaseColumn.setData(new Object());
        assertNotNull(abstractBaseColumn.getData());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asYearIntWhenDataIsNullThenReturnNull() {
        Integer result = abstractBaseColumn.asYearInt();
        assertNull(result);
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asDoubleWhenDataIsNullThenReturnNull() {
        assertNull(abstractBaseColumn.asDouble());
    }

    @Test
    @DisplayName("Should return null when the data is null")
    public void asDateWhenDataIsNullThenReturnNull() {
        Date result = abstractBaseColumn.asDate();
        assertNull(result);
    }

    @Test
    @DisplayName("Should return null when data is null")
    public void asFloatWhenDataIsNullThenReturnNull() {
        Float result = abstractBaseColumn.asFloat();
        assertNull(result);
    }

    @Test
    @DisplayName("Should return null when data is null")
    public void asLongWhenDataIsNullThenReturnNull() {
        Long result = abstractBaseColumn.asLong();
        assertNull(result);
    }

    @Test
    @DisplayName("Should return null when data is null")
    public void asShortWhenDataIsNullThenReturnNull() {

        Short result = abstractBaseColumn.asShort();

        assertNull(result);
    }

    @Test
    @DisplayName("Should return null when data is null")
    public void asIntWhenDataIsNullThenReturnNull() {
        Integer result = abstractBaseColumn.asInt();

        assertNull(result);
    }
}
