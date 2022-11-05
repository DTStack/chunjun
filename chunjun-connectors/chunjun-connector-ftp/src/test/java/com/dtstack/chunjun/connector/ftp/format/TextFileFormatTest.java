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

package com.dtstack.chunjun.connector.ftp.format;

import com.dtstack.chunjun.connector.ftp.client.File;

import org.apache.commons.io.input.BrokenInputStream;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import static org.junit.Assert.assertThrows;

public class TextFileFormatTest {

    private TextFileFormat textFileFormatUnderTest;

    @Before
    public void setUp() {
        textFileFormatUnderTest = new TextFileFormat();
    }

    @Test
    public void testOpen_BrokenInputStream() {
        // Setup
        final File file =
                new File("fileCompressPath", "fileAbsolutePath", "fileName", "compressType");
        final InputStream inputStream = new BrokenInputStream();
        final IFormatConfig config = new IFormatConfig();
        config.setFirstLineHeader(false);
        config.setFileConfig(new HashMap<>());
        config.setFieldDelimiter("fieldDelimiter");
        config.setEncoding("encoding");
        config.setFields(new String[] {"fields"});

        // Run the test
        assertThrows(
                IOException.class, () -> textFileFormatUnderTest.open(file, inputStream, config));
    }

    @Test
    public void testClose() throws Exception {
        // Setup
        // Run the test
        textFileFormatUnderTest.close();

        // Verify the results
    }
}
