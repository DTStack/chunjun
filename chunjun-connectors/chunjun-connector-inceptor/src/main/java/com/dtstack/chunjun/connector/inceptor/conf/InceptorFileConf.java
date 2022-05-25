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

package com.dtstack.chunjun.connector.inceptor.conf;

import com.dtstack.chunjun.conf.BaseFileConf;

import org.apache.parquet.hadoop.ParquetWriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InceptorFileConf extends BaseFileConf {

    protected String defaultFs;
    protected String fileType;
    protected String fieldDelimiter;
    protected Map<String, Object> hadoopConfig = new HashMap<>();
    protected String filterRegex;
    protected boolean transaction = false;

    // write
    protected int rowGroupSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
    private List<String> fullColumnName;
    private List<String> fullColumnType;
    private boolean enableDictionary = true;
    private String schema;
    private String table;
    private String partitionName;
    /** 目前只支持UTF-8 */
    protected String charsetName = "UTF-8";

    public String getDefaultFs() {
        return defaultFs;
    }

    public void setDefaultFs(String defaultFs) {
        this.defaultFs = defaultFs;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
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

    public boolean isTransaction() {
        return transaction;
    }

    public void setTransaction(boolean transaction) {
        this.transaction = transaction;
    }

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
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

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }
}
