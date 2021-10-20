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
package com.dtstack.flinkx.conf;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.sink.WriteMode;

import java.nio.charset.StandardCharsets;

/**
 * Date: 2021/06/08 Company: www.dtstack.com
 *
 * @author tudou
 */
public class BaseFileConf extends FlinkxCommonConf {

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

    @Override
    public String toString() {
        return "BaseFileConf{"
                + "path='"
                + path
                + '\''
                + ", fileName='"
                + fileName
                + '\''
                + ", writeMode='"
                + writeMode
                + '\''
                + ", compress='"
                + compress
                + '\''
                + ", encoding='"
                + encoding
                + '\''
                + ", maxFileSize="
                + maxFileSize
                + ", nextCheckRows="
                + nextCheckRows
                + '}';
    }
}
