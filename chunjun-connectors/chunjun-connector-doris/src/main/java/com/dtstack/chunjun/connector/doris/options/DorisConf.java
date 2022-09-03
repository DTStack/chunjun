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

import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.conf.SinkConnectionConf;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.MapUtil;
import com.dtstack.chunjun.util.StringUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * @author tiezhu@dtstack
 * @date 2021/9/16 星期四
 */
public class DorisConf extends JdbcConf {

    private String database;

    private String table;

    private String username;

    private String password;

    private String writeMode;

    private List<String> feNodes;

    private String url;

    /** * default value is 3 */
    private Integer maxRetries = 3;
    /** retry load sleep timeout* */
    private long waitRetryMills = 18000;

    /** 是否配置了NameMapping, true, RowData中将携带名称匹配后的数据库和表名, sink端配置的database和table失效* */
    private boolean nameMapped;

    private LoadConf loadConf;

    private Properties loadProperties;

    public long getWaitRetryMills() {
        return waitRetryMills;
    }

    public void setWaitRetryMills(long waitRetryMills) {
        this.waitRetryMills = waitRetryMills;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return Objects.isNull(password) ? "" : password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public List<String> getFeNodes() {
        return feNodes;
    }

    public void setFeNodes(List<String> feNodes) {
        this.feNodes = feNodes;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(Integer maxRetries) {
        this.maxRetries = maxRetries;
    }

    public LoadConf getLoadConf() {
        return loadConf;
    }

    public void setLoadConf(LoadConf loadConf) {
        this.loadConf = loadConf;
    }

    public Properties getLoadProperties() {
        return loadProperties;
    }

    public void setLoadProperties(Properties loadProperties) {
        this.loadProperties = loadProperties;
    }

    public boolean isNameMapped() {
        return nameMapped;
    }

    public void setNameMapped(boolean nameMapped) {
        this.nameMapped = nameMapped;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String serializeToString() {
        try {
            String optionsJson = GsonUtil.GSON.toJson(this);
            Properties optionsProperties = MapUtil.jsonStrToObject(optionsJson, Properties.class);
            return StringUtil.propsToString(optionsProperties);
        } catch (IOException e) {
            throw new IllegalArgumentException("Doris Options Serialize to String failed.", e);
        }
    }

    public JdbcConf setToJdbcConf() {
        JdbcConf jdbcConf = new JdbcConf();
        SinkConnectionConf connectionConf = new SinkConnectionConf();
        connectionConf.setJdbcUrl(url);
        connectionConf.setPassword(password);
        connectionConf.setSchema(database);
        connectionConf.setTable(Collections.singletonList(table));
        connectionConf.setUsername(username);
        jdbcConf.setConnection(Collections.singletonList(connectionConf));
        jdbcConf.setJdbcUrl(url);
        jdbcConf.setPassword(password);
        jdbcConf.setUsername(username);

        jdbcConf.setBatchSize(this.getBatchSize());
        jdbcConf.setFlushIntervalMills(this.getFlushIntervalMills());

        return jdbcConf;
    }
}
