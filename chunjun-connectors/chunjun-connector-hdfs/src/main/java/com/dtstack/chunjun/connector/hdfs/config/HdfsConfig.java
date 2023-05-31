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
package com.dtstack.chunjun.connector.hdfs.config;

import com.dtstack.chunjun.config.BaseFileConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;
import parquet.hadoop.ParquetWriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class HdfsConfig extends BaseFileConfig {

    private static final long serialVersionUID = 5718545511467772229L;

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
    private String finishedFileName;
}
