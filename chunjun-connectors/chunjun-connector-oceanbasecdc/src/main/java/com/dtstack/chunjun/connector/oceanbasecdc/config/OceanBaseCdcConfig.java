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

package com.dtstack.chunjun.connector.oceanbasecdc.config;

import com.dtstack.chunjun.config.CommonConfig;

import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class OceanBaseCdcConfig extends CommonConfig {
    private static final long serialVersionUID = -3826059387486062675L;

    private String logProxyHost;
    private int logProxyPort;
    private ObReaderConfig obReaderConfig;
    private String cat;
    private boolean pavingData = true;
    private boolean splitUpdate;

    private String timestampFormat = "sql";
}
