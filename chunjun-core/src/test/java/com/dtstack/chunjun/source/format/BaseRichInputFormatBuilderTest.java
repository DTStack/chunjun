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

import com.dtstack.chunjun.conf.ChunJunCommonConf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseRichInputFormatBuilderTest {

    private BaseRichInputFormatBuilder<MockInputFormat> builder;

    @BeforeEach
    public void setup() {
        builder = new MockInputFormatBuilder();
    }

    @Test
    public void testSetRowConverter() {
        MockRowConverter mockRowConverter = new MockRowConverter();
        builder.setRowConverter(mockRowConverter);
        BaseRichInputFormat mockInputFormat = builder.finish();
        assertEquals(mockRowConverter, mockInputFormat.rowConverter);
        assertFalse(mockInputFormat.useAbstractColumn);
    }

    @Test
    public void testSetRowConverterAndUseAbstractColumn() {
        MockRowConverter mockRowConverter = new MockRowConverter();
        builder.setRowConverter(mockRowConverter, true);
        BaseRichInputFormat mockInputFormatA = builder.finish();
        assertEquals(mockRowConverter, mockInputFormatA.rowConverter);
        assertTrue(mockInputFormatA.useAbstractColumn);

        builder.setRowConverter(mockRowConverter, false);
        BaseRichInputFormat mockInputFormatB = builder.finish();
        assertEquals(mockRowConverter, mockInputFormatB.rowConverter);
        assertFalse(mockInputFormatB.useAbstractColumn);
    }

    @Test
    public void testSetChunJunCommonConf() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        builder.setConfig(chunJunCommonConf);
        BaseRichInputFormat mockInputFormat = builder.finish();
        assertEquals(chunJunCommonConf, mockInputFormat.config);
    }

    @Test
    public void testCheckFormat() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setCheckFormat(true);
        builder.setConfig(chunJunCommonConf);
        MockInputFormatBuilder mockBuilder = (MockInputFormatBuilder) builder;
        assertFalse(mockBuilder.isChecked());
        builder.finish();
        assertTrue(mockBuilder.isChecked());
    }

    @Test
    public void testDoNotCheckFormat() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setCheckFormat(false);
        builder.setConfig(chunJunCommonConf);
        MockInputFormatBuilder mockBuilder = (MockInputFormatBuilder) builder;
        assertFalse(mockBuilder.isChecked());
        builder.finish();
        assertFalse(mockBuilder.isChecked());
    }
}
