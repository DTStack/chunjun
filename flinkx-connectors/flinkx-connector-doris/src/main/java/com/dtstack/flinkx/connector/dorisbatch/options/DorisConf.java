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

package com.dtstack.flinkx.connector.dorisbatch.options;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.MapUtil;
import com.dtstack.flinkx.util.StringUtil;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * @author tiezhu@dtstack
 * @date 2021/9/16 星期四
 */
public class DorisConf extends FlinkxCommonConf {

    private String fieldDelimiter;

    private String lineDelimiter;

    private String database;

    private String table;

    private String username;

    private String password;

    private String writeMode;

    private List<String> feNodes;

    /** * default value is 3 */
    private Integer maxRetries = 3;

    private LoadConf loadConf;

    private Properties loadProperties;

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public void setLineDelimiter(String lineDelimiter) {
        this.lineDelimiter = lineDelimiter;
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
        return password;
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

    public String serializeToString() {
        try {
            String optionsJson = GsonUtil.GSON.toJson(this);
            Properties optionsProperties = MapUtil.jsonStrToObject(optionsJson, Properties.class);
            return StringUtil.propsToString(optionsProperties);
        } catch (IOException e) {
            throw new IllegalArgumentException("Doris Options Serialize to String failed.", e);
        }
    }
}
