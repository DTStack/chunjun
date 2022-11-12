/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.conf;

import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.sink.WriteMode;

import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

public class BaseFileConfig extends CommonConfig {
    private int fromLine = 1;

    private String path;
    private String fileName;
    /** 写入模式 * */
    private String writeMode = WriteMode.APPEND.name();
    /** 压缩方式 */
    private String compress;

    private String encoding = StandardCharsets.UTF_8.name();
    private long maxFileSize = ConstantValue.STORE_SIZE_G;
    private long nextCheckRows = 5000;

    private String suffix;

    public int getFromLine() {
        return fromLine;
    }

    public void setFromLine(int fromLine) {
        this.fromLine = fromLine;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public String getCompress() {
        return compress;
    }

    public void setCompress(String compress) {
        this.compress = compress;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public long getNextCheckRows() {
        return nextCheckRows;
    }

    public void setNextCheckRows(long nextCheckRows) {
        this.nextCheckRows = nextCheckRows;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", BaseFileConfig.class.getSimpleName() + "[", "]")
                .add("fromLine=" + fromLine)
                .add("path='" + path + "'")
                .add("fileName='" + fileName + "'")
                .add("writeMode='" + writeMode + "'")
                .add("compress='" + compress + "'")
                .add("encoding='" + encoding + "'")
                .add("maxFileSize=" + maxFileSize)
                .add("nextCheckRows=" + nextCheckRows)
                .add("suffix='" + suffix + "'")
                .toString();
    }
}
