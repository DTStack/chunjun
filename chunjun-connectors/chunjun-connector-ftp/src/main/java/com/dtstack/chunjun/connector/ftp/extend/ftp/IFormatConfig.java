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

package com.dtstack.chunjun.connector.ftp.extend.ftp;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.StringJoiner;

public class IFormatConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String encoding;
    private String[] fields;
    private boolean isFirstLineHeader;
    private Map<String, Object> fileConfig;

    private int parallelism;
    private long fetchMaxSize;

    /* 列分隔符 */
    private String fieldDelimiter;

    /* 行分隔符 */
    private String columnDelimiter;

    public void setColumnDelimiter(String columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
    }

    public String getColumnDelimiter() {
        return columnDelimiter;
    }

    public boolean isFirstLineHeader() {
        return isFirstLineHeader;
    }

    public void setFirstLineHeader(boolean firstLineHeader) {
        isFirstLineHeader = firstLineHeader;
    }

    public Map<String, Object> getFileConfig() {
        return fileConfig;
    }

    public void setFileConfig(Map<String, Object> fileConfig) {
        this.fileConfig = fileConfig;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public void setFetchMaxSize(long fetchMaxSize) {
        this.fetchMaxSize = fetchMaxSize;
    }

    public long getFetchMaxSize() {
        return fetchMaxSize;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", IFormatConfig.class.getSimpleName() + "[", "]")
                .add("encoding='" + encoding + "'")
                .add("fields=" + Arrays.toString(fields))
                .add("isFirstLineHeader=" + isFirstLineHeader)
                .add("fileConfig=" + fileConfig)
                .add("parallelism=" + parallelism)
                .add("fetchMaxSize=" + fetchMaxSize)
                .add("fieldDelimiter='" + fieldDelimiter + "'")
                .add("columnDelimiter='" + columnDelimiter + "'")
                .toString();
    }
}
