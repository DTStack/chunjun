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

package com.dtstack.chunjun.metrics;

import org.apache.flink.api.common.accumulators.Accumulator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StringAccumulatorTest {

    private StringAccumulator accumulator;

    @BeforeEach
    void setUp() {
        this.accumulator = new StringAccumulator();
        this.accumulator.add("a");
    }

    @Test
    void testAdd() {
        assertEquals("a", this.accumulator.getLocalValue());
        this.accumulator.add("b");
        assertEquals("b", this.accumulator.getLocalValue());
    }

    @Test
    void testResetLocal() {
        this.accumulator.resetLocal();
        assertNull(this.accumulator.getLocalValue());
    }

    @Test
    void testClone() {
        Accumulator newAccumulator = this.accumulator.clone();
        assertNotEquals(this.accumulator, newAccumulator);

        assertEquals(this.accumulator.getLocalValue(), newAccumulator.getLocalValue());
    }

    @Test
    void testToString() {
        assertEquals("StringData a", this.accumulator.toString());
    }
}
