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

import lombok.NoArgsConstructor;

import java.math.BigInteger;

@NoArgsConstructor
public class BigIntegerAccumulator implements SimpleAccumulator<BigInteger> {

    private static final long serialVersionUID = 1L;

    public static Integer MIN_VAL = Integer.MIN_VALUE;

    private BigInteger max = BigInteger.valueOf(MIN_VAL);

    @Override
    public void add(BigInteger value) {
        if (this.max.compareTo(value) < 0) {
            this.max = value;
        }
    }

    @Override
    public BigInteger getLocalValue() {
        return this.max;
    }

    @Override
    public void merge(Accumulator<BigInteger, BigInteger> other) {
        if (this.max.compareTo(other.getLocalValue()) < 0) {
            this.max = other.getLocalValue();
        }
    }

    @Override
    public void resetLocal() {
        this.max = BigInteger.valueOf(Integer.MIN_VALUE);
    }

    @Override
    public BigIntegerAccumulator clone() {
        BigIntegerAccumulator clone = new BigIntegerAccumulator();
        clone.max = this.max;
        return clone;
    }

    @Override
    public String toString() {
        return "BigIntegerMaximum " + this.max.toString();
    }
}
