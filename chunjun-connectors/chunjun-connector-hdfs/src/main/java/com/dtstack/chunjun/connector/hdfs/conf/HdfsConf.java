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
package com.dtstack.chunjun.connector.hdfs.conf;

import com.dtstack.chunjun.conf.BaseFileConf;

import parquet.hadoop.ParquetWriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2021/06/08 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HdfsConf extends BaseFileConf {

    private String defaultFS;
    private String fileType;
    /** hadoop高可用相关配置 * */
    private Map<String, Object> hadoopConfig = new HashMap<>(16);

    private String filterRegex = "";
    private String fieldDelimiter = "\001";
    private int rowGroupSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
    private boolean enableDictionary = true;
    private List<String> fullColumnName;
    private List<String> fullColumnType;

    public String getDefaultFS() {
        return defaultFS;
    }

    public void setDefaultFS(String defaultFS) {
        this.defaultFS = defaultFS;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public Map<String, Object> getHadoopConfig() {
        return hadoopConfig;
    }

    public void setHadoopConfig(Map<String, Object> hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }

    public String getFilterRegex() {
        return filterRegex;
    }

    public void setFilterRegex(String filterRegex) {
        this.filterRegex = filterRegex;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public int getRowGroupSize() {
        return rowGroupSize;
    }

    public void setRowGroupSize(int rowGroupSize) {
        this.rowGroupSize = rowGroupSize;
    }

    public boolean isEnableDictionary() {
        return enableDictionary;
    }

    public void setEnableDictionary(boolean enableDictionary) {
        this.enableDictionary = enableDictionary;
    }

    public List<String> getFullColumnName() {
        return fullColumnName;
    }

    public void setFullColumnName(List<String> fullColumnName) {
        this.fullColumnName = fullColumnName;
    }

    public List<String> getFullColumnType() {
        return fullColumnType;
    }

    public void setFullColumnType(List<String> fullColumnType) {
        this.fullColumnType = fullColumnType;
    }

    @Override
    public String toString() {
        return "HdfsConf{"
                + "defaultFS='"
                + defaultFS
                + '\''
                + ", fileType='"
                + fileType
                + '\''
                + ", hadoopConfig="
                + hadoopConfig
                + ", filterRegex='"
                + filterRegex
                + '\''
                + ", fieldDelimiter='"
                + fieldDelimiter
                + '\''
                + ", rowGroupSize="
                + rowGroupSize
                + ", enableDictionary="
                + enableDictionary
                + ", fullColumnName="
                + fullColumnName
                + ", fullColumnType="
                + fullColumnType
                + '}';
    }
}
