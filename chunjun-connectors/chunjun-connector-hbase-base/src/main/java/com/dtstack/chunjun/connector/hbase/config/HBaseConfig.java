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

/*
 *    Copyright 2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.dtstack.chunjun.connector.hbase.config;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class HBaseConfig extends CommonConfig {

    private static final long serialVersionUID = 2773610621793371485L;
    // 该字段与 column 不同，该字段储存的是 ":" 转化为 "." 后的字段名
    private List<FieldConfig> columnMetaInfos;
    private String encoding = "UTF-8";
    private Map<String, Object> hbaseConfig;

    // reader
    private String startRowkey;
    private String endRowkey;
    private boolean isBinaryRowkey;
    private String table;
    private int scanCacheSize = 1000;
    private int scanBatchSize = -1;
    private int maxVersion = Integer.MAX_VALUE;
    private String mode = "normal";

    // writer
    private String nullMode = "SKIP";
    private String nullStringLiteral;
    private Boolean walFlag = false;
    private long writeBufferSize;
    private String rowkeyExpress;
    private Integer versionColumnIndex;
    private String versionColumnName;
    private String versionColumnValue;
    private Long ttl;
}
