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

package com.dtstack.flinkx.oss.writer;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.outputformat.FileOutputFormatBuilder;

import java.util.List;

/**
 * The builder class of HdfsOutputFormat
 *
 * @author wangyulei
 * @date 2021-06-30
 */
public class OssOutputFormatBuilder extends FileOutputFormatBuilder {

    private BaseOssOutputFormat format;

    public OssOutputFormatBuilder(String type) {
        switch (type.toUpperCase()) {
            case "TEXT":
                format = new OssTextOutputFormat();
                break;
            default:
                throw new IllegalArgumentException("Unsupported Oss file type: " + type);
        }

        super.setFormat(format);
    }

    public void setColumnNames(List<String> columnNames) {
        format.columnNames = columnNames;
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.columnTypes = columnTypes;
    }

    public void setEndpoint(String endpoint) {
        format.endpoint = endpoint;
    }

    public void setAccessKey(String accessKey) {
        format.accessKey = accessKey;
    }

    public void setSecretKey(String secretKey) {
        format.secretKey = secretKey;
    }

    public void setFullColumnNames(List<String> fullColumnNames) {
        format.fullColumnNames = fullColumnNames;
    }

    public void setDelimiter(String delimiter) {
        format.delimiter = delimiter;
    }

    public void setRowGroupSize(int rowGroupSize){
        format.rowGroupSize = rowGroupSize;
    }

    public void setFullColumnTypes(List<String> fullColumnTypes) {
        format.fullColumnTypes = fullColumnTypes;
    }

    public void setEnableDictionary(boolean enableDictionary) {
        format.enableDictionary = enableDictionary;
    }

    @Override
    protected void checkFormat() {
        super.checkFormat();

        if (super.format.getPath() == null || super.format.getPath().length() == 0) {
            throw new IllegalArgumentException("No valid path supplied.");
        }

        if (!super.format.getPath().startsWith(ConstantValue.PROTOCOL_S3A)) {
            throw new IllegalArgumentException("Path should start with s3a://");
        }
    }

}
