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
package com.dtstack.flinkx.connector.jdbc.conf;

import com.dtstack.flinkx.connector.jdbc.adapter.EncryptAdapter;

import com.google.gson.annotations.JsonAdapter;

import java.io.Serializable;

/**
 * Date: 2022/01/12 Company: www.dtstack.com
 *
 * @author tudou
 */
public class DataSourceConf implements Serializable {

    private static final long serialVersionUID = 1L;

    private String jdbcUrl;
    private String userName;

    @JsonAdapter(EncryptAdapter.class)
    private String password;

    private String table;

    private String schema;

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String toString() {
        return "DataSourceConf{"
                + "jdbcUrl='"
                + jdbcUrl
                + '\''
                + ", userName='"
                + userName
                + '\''
                + ", password='******"
                + '\''
                + ", table='"
                + table
                + '\''
                + ", schema='"
                + schema
                + '\''
                + '}';
    }
}
