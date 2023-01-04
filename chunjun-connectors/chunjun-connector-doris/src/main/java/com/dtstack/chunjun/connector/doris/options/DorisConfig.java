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

package com.dtstack.chunjun.connector.doris.options;

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.config.SinkConnectionConfig;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.MapUtil;
import com.dtstack.chunjun.util.StringUtil;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@EqualsAndHashCode(callSuper = true)
@Data
public class DorisConfig extends JdbcConfig {

    private static final long serialVersionUID = 3732649877755197591L;

    private String database;

    private String table;

    private List<String> feNodes;

    private String url;

    /** default value is 3 */
    private Integer maxRetries = 3;
    /** retry load sleep timeout* */
    private long waitRetryMills = 18000;

    /** 是否配置了NameMapping, true, RowData中将携带名称匹配后的数据库和表名, sink端配置的database和table失效* */
    private boolean nameMapped;

    private LoadConfig loadConfig;

    private Properties loadProperties;

    public String serializeToString() {
        try {
            String optionsJson = GsonUtil.GSON.toJson(this);
            Properties optionsProperties = MapUtil.jsonStrToObject(optionsJson, Properties.class);
            return StringUtil.propsToString(optionsProperties);
        } catch (IOException e) {
            throw new IllegalArgumentException("Doris Options Serialize to String failed.", e);
        }
    }

    public JdbcConfig setToJdbcConf() {
        JdbcConfig jdbcConfig = new JdbcConfig();
        SinkConnectionConfig connectionConf = new SinkConnectionConfig();
        connectionConf.setJdbcUrl(url);
        connectionConf.setPassword(password);
        connectionConf.setSchema(database);
        connectionConf.setTable(Collections.singletonList(table));
        connectionConf.setUsername(username);
        jdbcConfig.setConnection(Collections.singletonList(connectionConf));
        jdbcConfig.setJdbcUrl(url);
        jdbcConfig.setPassword(password);
        jdbcConfig.setUsername(username);

        jdbcConfig.setBatchSize(this.getBatchSize());
        jdbcConfig.setFlushIntervalMills(this.getFlushIntervalMills());

        return jdbcConfig;
    }
}
