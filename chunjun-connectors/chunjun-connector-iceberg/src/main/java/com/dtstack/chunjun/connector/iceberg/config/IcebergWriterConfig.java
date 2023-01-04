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

package com.dtstack.chunjun.connector.iceberg.config;

import com.dtstack.chunjun.config.CommonConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class IcebergWriterConfig extends CommonConfig {
    private static final long serialVersionUID = 185189430068911443L;

    private String defaultFS;

    private String fileType;

    /** hadoop高可用相关配置 * */
    private Map<String, Object> hadoopConfig = new HashMap<>(16);

    private String path;

    private String writeMode;
    /** upsert模式需要传入主键列表 */
    private List<String> primaryKey = new ArrayList<>();

    public static final String APPEND_WRITE_MODE = "append";
    public static final String UPSERT_WRITE_MODE = "upsert";
    public static final String OVERWRITE_WRITE_MODE = "overwrite";
}
