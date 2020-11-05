/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.hive.writer;

import com.dtstack.flinkx.hive.TableInfo;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import org.apache.commons.lang.StringUtils;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;

/**
 * @author toutian
 */
public class HiveOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    protected HiveOutputFormat format;


    public HiveOutputFormatBuilder() {
        format = new HiveOutputFormat();
        super.format = format;
    }

    public void setFileType(String fileType) {
        this.format.fileType = fileType;
    }

    public void setPartition(String partition) {
        this.format.partition = partition;
    }

    public void setPartitionType(String partitionType) {
        this.format.partitionType = partitionType;
    }

    /**
     * 字节数量超过 bufferSize 时，outputFormat 进行一次 close，触发输出文件的合并
     */
    public void setBufferSize(long bufferSize) {
        this.format.bufferSize = bufferSize;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.format.jdbcUrl = jdbcUrl;
    }

    public void setUsername(String username) {
        this.format.username = username;
    }

    public void setPassword(String password) {
        this.format.password = password;
    }

    public void setTableBasePath(String tableBasePath) {
        this.format.tableBasePath = tableBasePath;
    }

    public void setAutoCreateTable(boolean autoCreateTable) {
        this.format.autoCreateTable = autoCreateTable;
    }

    public void setTableInfos(Map<String, TableInfo> tableInfos) {
        this.format.tableInfos = tableInfos;
    }

    public void setDistributeTableMapping(Map<String, String> distributeTableMapping) {
        this.format.distributeTableMapping = distributeTableMapping;
    }
    
    

    public void setHadoopConfig(Map<String,Object> hadoopConfig) {
        format.hadoopConfig = hadoopConfig;
    }

    public void setDelimiter(String delimiter) {
        format.delimiter = delimiter;
    }

    public void setRowGroupSize(int rowGroupSize){
        format.rowGroupSize = rowGroupSize;
    }

    public void setDefaultFs(String defaultFs) {
        format.defaultFs = defaultFs;
    }

    public void setWriteMode(String writeMode) {
        this.format.writeMode = StringUtils.isBlank(writeMode) ? "APPEND" : writeMode.toUpperCase();
    }

    public void setCompress(String compress) {
        this.format.compress = compress;
    }

    public void setCharSetName(String charsetName) {
        if (StringUtils.isNotEmpty(charsetName)) {
            if (!Charset.isSupported(charsetName)) {
                throw new UnsupportedCharsetException("The charset " + charsetName + " is not supported.");
            }
            this.format.charsetName = charsetName;
        }

    }

    public void setMaxFileSize(long maxFileSize){
        this.format.maxFileSize = maxFileSize;
    }

    public void setSchema(String schema){
        format.schema = schema;
    }

    @Override
    protected void checkFormat() {
        if (this.format.tableBasePath == null || this.format.tableBasePath.length() == 0) {
            throw new IllegalArgumentException("No tableBasePath supplied.");
        }

        if (this.format.tableInfos.isEmpty()){
            throw new IllegalArgumentException("No tableInfos supplied.");
        }

        notSupportBatchWrite("HiveWriter");
    }

}
