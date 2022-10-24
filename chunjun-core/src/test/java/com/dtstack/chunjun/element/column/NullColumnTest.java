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

import static org.junit.jupiter.api.Assertions.assertNull;

class NullColumnTest {

    @Test
    @DisplayName("Should return null when the column is null")
    public void asTimestampStrWhenColumnIsNullThenReturnNull() {
        NullColumn nullColumn = new NullColumn();
        assertNull(nullColumn.asTimestampStr());
    }

    @Test
    @DisplayName("Should return null")
    public void asSqlDateShouldReturnNull() {
        NullColumn nullColumn = new NullColumn();
        assertNull(nullColumn.asSqlDate());
    }

    @Test
    @DisplayName("Should return null")
    public void asTimeShouldReturnNull() {
        NullColumn nullColumn = new NullColumn();
        assertNull(nullColumn.asTime());
    }

    @Test
    @DisplayName("Should return null")
    public void asTimestampShouldReturnNull() {
        NullColumn nullColumn = new NullColumn();
        assertNull(nullColumn.asTimestamp());
    }

    @Test
    @DisplayName("Should return null")
    public void asDoubleShouldReturnNull() {
        NullColumn nullColumn = new NullColumn();
        assertNull(nullColumn.asDouble());
    }

    @Test
    @DisplayName("Should return null")
    public void asBigDecimalShouldReturnNull() {
        NullColumn nullColumn = new NullColumn();
        assertNull(nullColumn.asBigDecimal());
    }

    @Test
    @DisplayName("Should return null")
    public void asBooleanShouldReturnNull() {
        NullColumn nullColumn = new NullColumn();
        assertNull(nullColumn.asBoolean());
    }

    @Test
    @DisplayName("Should return null")
    public void asBytesShouldReturnNull() {
        NullColumn nullColumn = new NullColumn();
        assertNull(nullColumn.asBytes());
    }

    @Test
    @DisplayName("Should return null")
    public void asStringShouldReturnNull() {
        NullColumn nullColumn = new NullColumn();
        assertNull(nullColumn.asString());
    }

    @Test
    @DisplayName("Should return null")
    public void asDateShouldReturnNull() {
        NullColumn nullColumn = new NullColumn();
        assertNull(nullColumn.asDate());
    }
}
