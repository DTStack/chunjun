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


package com.dtstack.flinkx.rdb.inputformat;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.accumulators.Accumulator;

import java.math.BigInteger;

/**
 * @author jiangbo
 * @explanation
 * @date 2019/1/17
 */
public class MaximumAccumulator implements Accumulator<String,String> {

    private String localValue;

    @Override
    public void add(String value) {
        if(StringUtils.isEmpty(value)){
            return;
        }

        if(localValue == null){
            localValue = value;
        } else if(NumberUtils.isNumber(localValue)){
            BigInteger newVal = new BigInteger(value);
            if(newVal.compareTo(new BigInteger(localValue)) > 0){
                localValue = value;
            }
        } else {
            localValue = localValue.compareTo(value) < 0 ? value : localValue;
        }
    }

    @Override
    public String getLocalValue() {
        return localValue;
    }

    @Override
    public void resetLocal() {
        localValue = null;
    }

    @Override
    public void merge(Accumulator<String, String> other) {
        if (other == null || StringUtils.isEmpty(other.getLocalValue())){
            return;
        }

        if (localValue == null){
            localValue = other.getLocalValue();
            return;
        }

        if(NumberUtils.isNumber(localValue)){
            BigInteger local = new BigInteger(localValue);
            if(local.compareTo(new BigInteger(other.getLocalValue())) < 0){
                localValue = other.getLocalValue();
            }
        } else {
            localValue = localValue.compareTo(other.getLocalValue()) < 0 ? other.getLocalValue() : localValue;
        }
    }

    @Override
    public Accumulator<String, String> clone() {
        MaximumAccumulator maximumAccumulator = new MaximumAccumulator();
        maximumAccumulator.add(localValue);
        return maximumAccumulator;
    }
}
