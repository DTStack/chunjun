/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.influxdb.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Locale;

@EqualsAndHashCode(callSuper = true)
@Data
public class InfluxdbSourceConfig extends InfluxdbConfig {

    private static final long serialVersionUID = -3007933210720629076L;

    private String where;
    private String customSql;
    private String splitPk;
    private int queryTimeOut = 3;
    private int fetchSize = 1000;
    private String epoch = "N";
    private String format = "MSGPACK";

    public String getFormat() {
        return format.toUpperCase(Locale.ENGLISH);
    }

    public String getEpoch() {
        return epoch.toLowerCase(Locale.ENGLISH);
    }
}
