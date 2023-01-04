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

import com.dtstack.chunjun.config.CommonConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class InfluxdbConfig extends CommonConfig {

    private static final long serialVersionUID = -7593820652819230606L;

    private String username;
    private String password;
    private List<Connection> connection;

    public String getMeasurement() {
        return connection.get(0).getMeasurement().get(0);
    }

    public String getDatabase() {
        return connection.get(0).getDatabase();
    }

    public List<String> getUrl() {
        return connection.get(0).getUrl();
    }

    @Data
    public static class Connection implements Serializable {
        private static final long serialVersionUID = 6950334044173729484L;

        private List<String> url;
        private List<String> measurement;
        private String database;
    }
}
