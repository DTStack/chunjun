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
package com.dtstack.flinkx.rdb;

import java.io.Serializable;

/**
 * @author jiangbo
 */
public class DataSource implements Serializable {

    private String jdbcUrl;
    private String userName;
    private String password;
    private String table;
    private boolean isSplitByKey;
    private Object[] parameterValues;
    private boolean isFinished;

    public DataSource() {
        this.isSplitByKey = false;
        this.isFinished = false;
    }

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

    public boolean isSplitByKey() {
        return isSplitByKey;
    }

    public void setSplitByKey(boolean splitByKey) {
        isSplitByKey = splitByKey;
    }

    public Object[] getParameterValues() {
        return parameterValues;
    }

    public void setParameterValues(Object[] parameterValues) {
        this.parameterValues = parameterValues;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void setFinished(boolean finished) {
        isFinished = finished;
    }
}
