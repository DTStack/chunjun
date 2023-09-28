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

package com.dtstack.chunjun.dirty.consumer;

import com.dtstack.chunjun.dirty.DirtyConfig;
import com.dtstack.chunjun.dirty.impl.DirtyDataEntry;
import com.dtstack.chunjun.throwable.NoRestartException;

import org.apache.flink.api.common.accumulators.LongCounter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DirtyDataCollectorTest {

    private DirtyDataCollector dirtyDataCollector;

    @BeforeEach
    void setUp() {
        dirtyDataCollector =
                new DirtyDataCollector() {
                    @Override
                    protected void init(DirtyConfig conf) {}

                    @Override
                    protected void consume(DirtyDataEntry dirty) throws Exception {}

                    @Override
                    public void close() {}
                };
    }

    @Test
    @DisplayName("Should return the consumed counter")
    void getConsumedShouldReturnTheConsumedCounter() {
        assertEquals(0L, dirtyDataCollector.getConsumed().getLocalValue());
    }

    @Test
    @DisplayName("Should return the failed consumed counter")
    void getFailedConsumedShouldReturnTheFailedConsumedCounter() {
        LongCounter failedConsumedCounter = dirtyDataCollector.getFailedConsumed();
        assertEquals(0L, failedConsumedCounter.getLocalValue());
    }

    @Test
    @DisplayName("Should throw an exception when the consumed count exceed the max-consumed")
    void addConsumedWhenConsumedCountExceedMaxConsumedThenThrowException() {
        dirtyDataCollector.maxConsumed = 2L;
        DirtyDataEntry dirty = new DirtyDataEntry();
        dirty.setDirtyContent("{}");
        dirtyDataCollector.addConsumed(1L, dirty, 0L);
        assertThrows(NoRestartException.class, () -> dirtyDataCollector.addConsumed(1L, dirty, 0));
    }

    @Test
    @DisplayName(
            "Should throw an exception when the failed consumed count exceed the max failed consumed")
    void addFailedConsumedWhenFailedConsumedCountExceedMaxFailedConsumedThenThrowException() {
        dirtyDataCollector.maxFailedConsumed = 2L;
        dirtyDataCollector.addFailedConsumed(new Exception(), 1L);
        assertThrows(
                NoRestartException.class,
                () -> dirtyDataCollector.addFailedConsumed(new Exception(), 1L));
    }

    @Test
    @DisplayName("Should add the dirty data to the queue")
    void offerShouldAddDirtyDataToQueue() {
        dirtyDataCollector.maxConsumed = 2L;
        DirtyDataEntry dirtyDataEntry = new DirtyDataEntry();
        dirtyDataCollector.offer(dirtyDataEntry, 0);
        assertEquals(1, dirtyDataCollector.consumeQueue.size());
    }

    @Test
    @DisplayName("Should increase the consumed counter by 1")
    void offerShouldIncreaseConsumedCounterBy1() {
        dirtyDataCollector.maxConsumed = 2L;
        dirtyDataCollector.offer(new DirtyDataEntry(), 0);
        assertEquals(1L, dirtyDataCollector.getConsumed().getLocalValue());
    }

    @Test
    @DisplayName("Should set maxconsumed to the value of conf.maxconsumed")
    void initializeConsumerShouldSetMaxConsumedToTheValueOfConfMaxConsumed() {
        DirtyConfig conf = new DirtyConfig();
        conf.setMaxConsumed(10L);
        dirtyDataCollector.initializeConsumer(conf);
        assertEquals(10L, dirtyDataCollector.maxConsumed);
    }

    @Test
    @DisplayName("Should set maxfailedconsumed to the value of conf.maxfailedconsumed")
    void initializeConsumerShouldSetMaxFailedConsumedToTheValueOfConfMaxFailedConsumed() {
        DirtyConfig conf = new DirtyConfig();
        conf.setMaxFailedConsumed(10L);
        dirtyDataCollector.initializeConsumer(conf);
        assertEquals(10L, dirtyDataCollector.maxFailedConsumed);
    }
}
