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

package com.dtstack.chunjun.connector.sqlservercdc.config;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class SqlServerCdcConfig extends CommonConfig {

    private static final long serialVersionUID = 4139763920268366783L;
    private String username;
    private String password;
    private String url;
    private String databaseName;
    private String cat;
    private boolean pavingData;
    private List<String> tableList;
    private Long pollInterval = 1000L;
    private String lsn;
    private boolean splitUpdate;
    private String timestampFormat = "sql";
    private List<FieldConfig> column;
    private boolean autoCommit = false;
    private boolean autoResetConnection = false;
}
