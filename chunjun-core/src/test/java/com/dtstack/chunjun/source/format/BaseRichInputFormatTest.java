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

package com.dtstack.chunjun.source.format;

import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.metrics.RowSizeCalculator;
import com.dtstack.chunjun.metrics.mock.MockReport;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.throwable.NoRestartException;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.dtstack.chunjun.source.DtInputFormatSourceFunctionTest.classloaderSafeInvoke;
import static com.dtstack.chunjun.source.format.MockInputFormat.MOCK_JOB_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseRichInputFormatTest {

    private BaseRichInputFormat inputFormat;

    /** some test will change TCCL, so we need to record and reset TCCL for each test */
    private ClassLoader oldClassLoader;

    @BeforeEach
    public void setupForEach() {
        oldClassLoader = Thread.currentThread().getContextClassLoader();
        inputFormat = new MockInputFormat();
    }

    @AfterEach
    public void AfterForEach() {
        Thread.currentThread().setContextClassLoader(oldClassLoader);
    }

    @Test
    public void testGetStatistics() {
        assertNull(
                inputFormat.getStatistics(
                        new BaseStatistics() {
                            @Override
                            public long getTotalInputSize() {
                                return 0;
                            }

                            @Override
                            public long getNumberOfRecords() {
                                return 0;
                            }

                            @Override
                            public float getAverageRecordWidth() {
                                return 0;
                            }
                        }));
    }

    @Test
    public void testOpen() {
        // reset thread classloader otherwise it will affect other test classes
        classloaderSafeInvoke(
                () -> {
                    RuntimeException thrown =
                            assertThrows(
                                    RuntimeException.class,
                                    () -> inputFormat.open(new ErrorInputSplit("error split")),
                                    "Expected open() to throw, but it didn't");

                    assertEquals("error split", thrown.getMessage());
                    assertDoesNotThrow(() -> inputFormat.open(new MockSplit(1, 1)));
                });
    }

    @Test
    public void testCreateInputSplits() {
        InputSplit[] errorSplits = inputFormat.createInputSplits(0);
        assertEquals(1, errorSplits.length);
        InputSplit errorSplit = errorSplits[0];
        assertEquals(ErrorInputSplit.class, errorSplit.getClass());

        InputSplit[] inputSplits = inputFormat.createInputSplits(3);
        assertTrue(inputSplits.length > 0);
    }

    @Test
    public void testOpenInputFormat() {
        assertDoesNotThrow(() -> inputFormat.openInputFormat());
        assertEquals(MOCK_JOB_NAME, inputFormat.jobName);
        assertEquals(MOCK_JOB_NAME, inputFormat.jobId);
        assertEquals(1, inputFormat.indexOfSubTask);
        assertEquals(MockReport.class, inputFormat.customReporter.getClass());
    }

    @Test
    public void testGetInputSplitAssigner() {
        InputSplitAssigner inputSplitAssigner =
                inputFormat.getInputSplitAssigner(new InputSplit[] {new MockSplit(1, 1)});
        assertEquals(DefaultInputSplitAssigner.class, inputSplitAssigner.getClass());
    }

    @Test
    public void testNextRecord() {
        // reset thread classloader otherwise it will affect other test classes
        classloaderSafeInvoke(
                () -> {
                    inputFormat.open(new MockSplit(1, 1));
                    RowData result = inputFormat.nextRecord(MockInputFormat.SUCCESS_DATA);
                    assertEquals(MockInputFormat.SUCCESS_DATA, result);
                    assertEquals(1L, inputFormat.numReadCounter.getLocalValue());
                    RowSizeCalculator rowSizeCalculator = inputFormat.rowSizeCalculator;
                    assertEquals(
                            rowSizeCalculator.getObjectSize(MockInputFormat.SUCCESS_DATA),
                            inputFormat.bytesReadCounter.getLocalValue());

                    NoRestartException thrown =
                            assertThrows(
                                    NoRestartException.class,
                                    () -> inputFormat.nextRecord(MockInputFormat.ERROR_DATA),
                                    "Expected nextRecord() to throw, but it didn't");
                    assertEquals(
                            "The dirty consumer shutdown, due to the consumed count exceed the max-consumed [0]",
                            thrown.getMessage());
                });
    }

    @Test
    public void testClose() {
        // reset thread classloader otherwise it will affect other test classes
        classloaderSafeInvoke(
                () -> {
                    inputFormat.open(new MockSplit(1, 1));
                    inputFormat.close();
                    assertFalse(inputFormat.dirtyManager.isAlive());
                });
    }

    @Test
    public void testCloseInputFormat() {
        // reset thread classloader otherwise it will affect other test classes
        classloaderSafeInvoke(
                () -> {
                    inputFormat.open(new MockSplit(1, 1));
                    inputFormat.closeInputFormat();
                    assertTrue(inputFormat.isClosed.get());
                });
    }

    @Test
    public void testUpdateDuration() {
        // reset thread classloader otherwise it will affect other test classes
        classloaderSafeInvoke(
                () -> {
                    inputFormat.open(new MockSplit(1, 1));
                    inputFormat.openInputFormat();
                    TimeUnit.MILLISECONDS.sleep(1000);
                    inputFormat.updateDuration();
                    long past = inputFormat.durationCounter.getLocalValue();
                    assertTrue(past > 1000L);
                });
    }

    @Test
    public void testInitRestoreInfo() {
        // reset thread classloader otherwise it will affect other test classes
        classloaderSafeInvoke(
                () -> {
                    FormatState formatState = new FormatState();
                    formatState.setMetric(
                            ImmutableMap.<String, LongCounter>builder()
                                    .put(Metrics.NUM_READS, new LongCounter(1000L))
                                    .put(Metrics.READ_BYTES, new LongCounter(2000L))
                                    .put(Metrics.READ_DURATION, new LongCounter(3000L))
                                    .build());
                    inputFormat.setFormatState(formatState);
                    inputFormat.open(new MockSplit(1, 1));

                    assertEquals(1000L, inputFormat.numReadCounter.getLocalValue());
                    assertEquals(2000L, inputFormat.bytesReadCounter.getLocalValue());
                    assertEquals(3000L, inputFormat.durationCounter.getLocalValue());
                });
    }

    @Test
    public void testGetFormatState() {
        // reset thread classloader otherwise it will affect other test classes
        classloaderSafeInvoke(
                () -> {
                    FormatState formatState = new FormatState();
                    formatState.setMetric(
                            ImmutableMap.<String, LongCounter>builder()
                                    .put(Metrics.NUM_READS, new LongCounter(0L))
                                    .put(Metrics.READ_BYTES, new LongCounter(0L))
                                    .put(Metrics.READ_DURATION, new LongCounter(0L))
                                    .build());
                    inputFormat.setFormatState(formatState);
                    inputFormat.open(new MockSplit(1, 1));
                    inputFormat.nextRecord(MockInputFormat.SUCCESS_DATA);
                    FormatState newFormatState = inputFormat.getFormatState();
                    Map<String, LongCounter> metric = newFormatState.getMetric();
                    assertEquals(1L, metric.get(Metrics.NUM_READS).getLocalValue());
                    RowSizeCalculator<Object> rowSizeCalculator =
                            RowSizeCalculator.getRowSizeCalculator();
                    assertEquals(
                            rowSizeCalculator.getObjectSize(MockInputFormat.SUCCESS_DATA),
                            metric.get(Metrics.READ_BYTES).getLocalValue());
                    assertTrue(metric.get(Metrics.READ_DURATION).getLocalValue() > 0L);
                });
    }
}
