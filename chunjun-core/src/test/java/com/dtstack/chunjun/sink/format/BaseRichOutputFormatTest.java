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

package com.dtstack.chunjun.sink.format;

import com.dtstack.chunjun.enums.Semantic;
import com.dtstack.chunjun.metrics.RowSizeCalculator;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.MockInputFormat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseRichOutputFormatTest {

    private BaseRichOutputFormat outputFormat;

    @BeforeEach
    public void setupForEach() {
        outputFormat = new MockBaseRichOutputFormat();
    }

    @Test
    public void testOpen() throws IOException {
        outputFormat.open(1, 2);
    }

    @Test
    @DisplayName("test writeRecord when batchSize equal 1")
    public void testWriteRecordWhenBatchSizeEqual1() throws IOException {
        outputFormat.open(1, 2);
        outputFormat.batchSize = 1;
        outputFormat.writeRecord(MockInputFormat.SUCCESS_DATA);
        assertEquals(1L, outputFormat.numWriteCounter.getLocalValue());
        RowSizeCalculator rowSizeCalculator = outputFormat.rowSizeCalculator;
        assertEquals(
                rowSizeCalculator.getObjectSize(MockInputFormat.SUCCESS_DATA),
                outputFormat.bytesWriteCounter.getLocalValue());
    }

    @Test
    @DisplayName("test writeRecord when batchSize large than 1")
    public void testWriteRecordWhenBatchSizeLargeThan1() throws IOException {
        outputFormat.open(1, 2);
        outputFormat.batchSize = 2;
        RowSizeCalculator rowSizeCalculator = outputFormat.rowSizeCalculator;
        outputFormat.writeRecord(MockInputFormat.SUCCESS_DATA);
        assertEquals(0L, outputFormat.numWriteCounter.getLocalValue());

        outputFormat.writeRecord(MockInputFormat.SUCCESS_DATA);
        assertEquals(2L, outputFormat.numWriteCounter.getLocalValue());
        assertEquals(
                rowSizeCalculator.getObjectSize(MockInputFormat.SUCCESS_DATA) * 2,
                outputFormat.bytesWriteCounter.getLocalValue());
    }

    @Test
    public void testClose() throws IOException {
        outputFormat.open(1, 2);
        outputFormat.close();
        assertTrue(outputFormat.closed);
    }

    @Test
    public void testNotifyCheckpointCompleteSuccess() throws IOException {
        outputFormat.open(1, 2);
        outputFormat.semantic = Semantic.EXACTLY_ONCE;
        assertDoesNotThrow(
                () ->
                        outputFormat.notifyCheckpointComplete(
                                MockBaseRichOutputFormat.SUCCESS_CHECK_POINT_ID));
        assertTrue(outputFormat.flushEnable.get());
    }

    @Test
    public void testNotifyCheckpointCompleteFail() throws IOException {
        outputFormat.open(1, 2);
        outputFormat.semantic = Semantic.EXACTLY_ONCE;
        assertDoesNotThrow(
                () ->
                        outputFormat.notifyCheckpointComplete(
                                MockBaseRichOutputFormat.FAIL_CHECK_POINT_ID));
        assertTrue(outputFormat.flushEnable.get());
    }

    @Test
    public void testNotifyCheckpointAbortedSuccess() throws IOException {
        outputFormat.open(1, 2);
        outputFormat.semantic = Semantic.EXACTLY_ONCE;
        assertDoesNotThrow(
                () ->
                        outputFormat.notifyCheckpointAborted(
                                MockBaseRichOutputFormat.SUCCESS_CHECK_POINT_ID));
        assertTrue(outputFormat.flushEnable.get());
    }

    @Test
    public void testNotifyCheckpointAbortedFail() throws IOException {
        outputFormat.open(1, 2);
        outputFormat.semantic = Semantic.EXACTLY_ONCE;
        assertDoesNotThrow(
                () ->
                        outputFormat.notifyCheckpointAborted(
                                MockBaseRichOutputFormat.FAIL_CHECK_POINT_ID));
        assertTrue(outputFormat.flushEnable.get());
    }

    @Test
    @DisplayName(
            "if semantic is exactly once,outputFormat will not flush data when getFormatState ")
    public void testGetFormatStateWhenExactlyOnce() throws Exception {
        outputFormat.open(1, 2);
        outputFormat.semantic = Semantic.EXACTLY_ONCE;
        outputFormat.batchSize = 3;
        outputFormat.writeRecord(MockInputFormat.SUCCESS_DATA);
        FormatState formatState = outputFormat.getFormatState();
        assertEquals(0, formatState.getNumberWrite());
    }

    @Test
    @DisplayName("if semantic is at least once,outputFormat will flush data when getFormatState ")
    public void testGetFormatStateWhenAtLeastOnce() throws Exception {
        outputFormat.open(1, 2);
        outputFormat.semantic = Semantic.AT_LEAST_ONCE;
        outputFormat.batchSize = 3;
        outputFormat.writeRecord(MockInputFormat.SUCCESS_DATA);
        FormatState formatState = outputFormat.getFormatState();
        assertEquals(1, formatState.getNumberWrite());
    }
}
