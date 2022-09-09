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
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

class BigIntegerAccmulatorTest {

    private SimpleAccumulator<BigInteger> accumulator;

    @BeforeEach
    void setUp() {
        this.accumulator = new BigIntegerAccumulator();
        this.accumulator.add(new BigInteger("3"));
    }

    @Test
    void testAdd() {
        assertEquals(new BigInteger("3"), this.accumulator.getLocalValue());
        this.accumulator.add(new BigInteger("4"));
        assertEquals(new BigInteger("4"), this.accumulator.getLocalValue());
        this.accumulator.add(new BigInteger("1"));
        assertEquals(new BigInteger("4"), this.accumulator.getLocalValue());
    }

    @Test
    void testMerge() {
        SimpleAccumulator<BigInteger> other1 = new BigIntegerAccumulator();
        other1.add(new BigInteger("1"));
        this.accumulator.merge(other1);
        assertEquals(new BigInteger("3"), this.accumulator.getLocalValue());

        SimpleAccumulator<BigInteger> other2 = new BigIntegerAccumulator();
        other2.add(new BigInteger("10"));
        this.accumulator.merge(other2);
        assertEquals(new BigInteger("10"), this.accumulator.getLocalValue());

        SimpleAccumulator<BigInteger> other3 = new BigIntegerAccumulator();
        other3.add(new BigInteger("5"));
        this.accumulator.merge(other3);
        assertEquals(new BigInteger("10"), this.accumulator.getLocalValue());
    }

    @Test
    void testResetLocal() {
        this.accumulator.resetLocal();
        assertEquals(BigInteger.valueOf(Integer.MIN_VALUE), this.accumulator.getLocalValue());
    }

    @Test
    void testClone() {
        Accumulator newAccumulator = this.accumulator.clone();
        assertNotEquals(this.accumulator, newAccumulator);

        assertEquals(this.accumulator.getLocalValue(), newAccumulator.getLocalValue());
    }

    @Test
    void testToString() {
        assertEquals("BigIntegerMaximum 3", this.accumulator.toString());
    }
}
