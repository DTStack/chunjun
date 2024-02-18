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

package com.dtstack.chunjun.config;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class BaseFileConfigTest {

    /** Should return a string with all the fields */
    @Test
    public void toStringShouldReturnAStringWithAllTheFields() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        baseFileConf.setPath("path");
        baseFileConf.setFileName("fileName");
        baseFileConf.setWriteMode("writeMode");
        baseFileConf.setCompress("compress");
        baseFileConf.setEncoding(StandardCharsets.UTF_8.name());
        baseFileConf.setMaxFileSize(1024L);
        baseFileConf.setNextCheckRows(5000L);

        String expected =
                "BaseFileConfig(fromLine=1, path=path, fileName=fileName, writeMode=writeMode, compress=compress, encoding=UTF-8, maxFileSize=1024, nextCheckRows=5000, suffix=null, jobIdentifier=)";

        assertEquals(expected, baseFileConf.toString());
    }

    /** Should return the nextCheckRows when the nextCheckRows is set */
    @Test
    public void getNextCheckRowsWhenNextCheckRowsIsSet() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        baseFileConf.setNextCheckRows(100);
        assertEquals(100, baseFileConf.getNextCheckRows());
    }

    /** Should return 5000 when the nextCheckRows is not set */
    @Test
    public void getNextCheckRowsWhenNextCheckRowsIsNotSet() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        assertEquals(5000, baseFileConf.getNextCheckRows());
    }

    /** Should return the maxFileSize when the maxFileSize is set */
    @Test
    public void getMaxFileSizeWhenMaxFileSizeIsSet() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        baseFileConf.setMaxFileSize(1024L);
        assertEquals(1024L, baseFileConf.getMaxFileSize());
    }

    /** Should return the default value when the maxFileSize is not set */
    @Test
    public void getMaxFileSizeWhenMaxFileSizeIsNotSet() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        baseFileConf.setMaxFileSize(1024);
        assertEquals(1024, baseFileConf.getMaxFileSize());
    }

    /** Should return the encoding when the encoding is set */
    @Test
    public void getEncodingWhenEncodingIsSet() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        baseFileConf.setEncoding("UTF-8");
        assertEquals("UTF-8", baseFileConf.getEncoding());
    }

    /** Should return UTF_8 when the encoding is not set */
    @Test
    public void getEncodingWhenEncodingIsNotSet() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        assertEquals(StandardCharsets.UTF_8.name(), baseFileConf.getEncoding());
    }

    /** Should return the compress when the compress is not null */
    @Test
    public void getCompressWhenCompressIsNotNull() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        baseFileConf.setCompress("gzip");
        assertEquals("gzip", baseFileConf.getCompress());
    }

    /** Should return null when the compress is null */
    @Test
    public void getCompressWhenCompressIsNull() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        baseFileConf.setCompress("gzip");
        assertEquals("gzip", baseFileConf.getCompress());
    }

    /** Should return the write mode */
    @Test
    public void getWriteModeShouldReturnTheWriteMode() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        baseFileConf.setWriteMode("append");
        assertEquals("append", baseFileConf.getWriteMode());
    }

    /** Should return the file name when the file name is not null */
    @Test
    public void getFileNameWhenFileNameIsNotNull() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        baseFileConf.setFileName("test.txt");
        assertEquals("test.txt", baseFileConf.getFileName());
    }

    /** Should return the path */
    @Test
    public void getPathShouldReturnThePath() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        baseFileConf.setPath("/tmp/test");
        assertEquals("/tmp/test", baseFileConf.getPath());
    }

    /** Should return 1 when fromLine is not set */
    @Test
    public void getFromLineWhenFromLineIsNotSetThenReturn1() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        assertEquals(1, baseFileConf.getFromLine());
    }

    /** Should return fromLine when fromLine is set */
    @Test
    public void getFromLineWhenFromLineIsSetThenReturnFromLine() {
        BaseFileConfig baseFileConf = new BaseFileConfig();
        baseFileConf.setFromLine(2);
        assertEquals(2, baseFileConf.getFromLine());
    }
}
