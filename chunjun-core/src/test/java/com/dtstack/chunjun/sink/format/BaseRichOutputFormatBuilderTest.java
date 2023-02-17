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

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.source.format.MockRowConverter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.dtstack.chunjun.constants.ConstantValue.MAX_BATCH_SIZE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseRichOutputFormatBuilderTest {

    @Test
    @DisplayName("should throw IllegalArgumentException when batch is large than MAX_BATCH_SIZE")
    public void testFinishWhenBatchSizeLargeMax() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setBatchSize(MAX_BATCH_SIZE + 1);
        MockBaseRichOutputFormat format = new MockBaseRichOutputFormat();
        format.setConfig(commonConfig);
        BaseRichOutputFormatBuilder builder = new MockBaseRichOutputFormatBuilder();
        builder.format = format;
        IllegalArgumentException thrown =
                assertThrows(
                        IllegalArgumentException.class,
                        builder::finish,
                        "Expected loadClass() to throw, but it didn't");
        assertEquals("批量写入条数必须小于[200000]条", thrown.getMessage());
    }

    @Test
    @DisplayName("should throw IllegalArgumentException when batch is less than MAX_BATCH_SIZE")
    public void testFinishWhenBatchSizeLessMax() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setBatchSize(MAX_BATCH_SIZE);
        MockBaseRichOutputFormat format = new MockBaseRichOutputFormat();
        format.setConfig(commonConfig);
        BaseRichOutputFormatBuilder builder = new MockBaseRichOutputFormatBuilder();
        builder.format = format;
        assertDoesNotThrow(builder::finish);
    }

    @Test
    @DisplayName("only set rowConverter")
    public void testSetRowConverter() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setBatchSize(MAX_BATCH_SIZE);
        MockBaseRichOutputFormat format = new MockBaseRichOutputFormat();
        format.setConfig(commonConfig);
        BaseRichOutputFormatBuilder builder = new MockBaseRichOutputFormatBuilder();
        builder.format = format;
        MockRowConverter mockRowConverter = new MockRowConverter();
        builder.setRowConverter(mockRowConverter);
        assertEquals(mockRowConverter, format.rowConverter);
        assertFalse(format.useAbstractColumn);
    }

    @Test
    @DisplayName("set rowConverter and ")
    public void testSetRowConverterAndUseAbstractColumn() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setBatchSize(MAX_BATCH_SIZE);
        MockBaseRichOutputFormat format = new MockBaseRichOutputFormat();
        format.setConfig(commonConfig);
        BaseRichOutputFormatBuilder builder = new MockBaseRichOutputFormatBuilder();
        builder.format = format;
        MockRowConverter mockRowConverter = new MockRowConverter();
        builder.setRowConverter(mockRowConverter, true);
        assertEquals(mockRowConverter, format.rowConverter);
        assertTrue(format.useAbstractColumn);
    }
}
