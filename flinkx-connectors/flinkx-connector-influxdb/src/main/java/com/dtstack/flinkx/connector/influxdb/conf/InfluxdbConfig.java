/*
 *
 *  *
 *  *  * Licensed to the Apache Software Foundation (ASF) under one
 *  *  * or more contributor license agreements.  See the NOTICE file
 *  *  * distributed with this work for additional information
 *  *  * regarding copyright ownership.  The ASF licenses this file
 *  *  * to you under the Apache License, Version 2.0 (the
 *  *  * "License"); you may not use this file except in compliance
 *  *  * with the License.  You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package com.dtstack.flinkx.connector.influxdb.conf;

import com.dtstack.flinkx.conf.FlinkxCommonConf;

import java.util.List;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2022/3/9
 */
public class InfluxdbConfig extends FlinkxCommonConf {

    private static final long serialVersionUID = 1L;

    private String username;
    private String password;
    private String format;
    private String where;
    private String customSql;
    private String splitPk;
    private int fetchSize;
    private int queryTimeOut;
    private String epoch;
    private List<Connection> connection;

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

    public String getUrl() {
        return connection.get(0).getUrl();
    }

    public String getMeasurement() {
        return connection.get(0).getMeasurement().get(0);
    }

    public String getDatabase() {
        return connection.get(0).getDatabase();
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public String getCustomSql() {
        return customSql;
    }

    public void setCustomSql(String customSql) {
        this.customSql = customSql;
    }

    public String getSplitPk() {
        return splitPk;
    }

    public void setSplitPk(String splitPk) {
        this.splitPk = splitPk;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int getQueryTimeOut() {
        return queryTimeOut;
    }

    public void setQueryTimeOut(int queryTimeOut) {
        this.queryTimeOut = queryTimeOut;
    }

    public String getEpoch() {
        return epoch;
    }

    public void setEpoch(String epoch) {
        this.epoch = epoch;
    }

    public void setConnection(List<Connection> connection) {
        this.connection = connection;
    }

    public static class Connection {
        private String url;
        private List<String> measurement;
        private String database;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public List<String> getMeasurement() {
            return measurement;
        }

        public void setMeasurement(List<String> measurement) {
            this.measurement = measurement;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }
    }
}
