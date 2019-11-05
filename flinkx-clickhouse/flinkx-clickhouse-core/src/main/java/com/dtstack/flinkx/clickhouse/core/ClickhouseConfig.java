/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.clickhouse.core;

import com.dtstack.flinkx.reader.MetaColumn;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

/**
 * Date: 2019/11/05
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class ClickhouseConfig implements Serializable {
    protected static final long serialVersionUID = 1L;

    //common
    private String url;
    private String username;
    private String password;
    private String table;
    private List<MetaColumn> column;
    private Properties clickhouseProp;

    //reader
    private Integer queryTimeOut;
    private String splitKey;
    private String filter;
    private String increColumn;
    private String startLocation;

    //writer
    private Integer batchInterval;
    private String preSql;
    private String postSql;


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
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

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<MetaColumn> getColumn() {
        return column;
    }

    public void setColumn(List<MetaColumn> column) {
        this.column = column;
    }

    public Properties getClickhouseProp() {
        return clickhouseProp;
    }

    public void setClickhouseProp(Properties clickhouseProp) {
        this.clickhouseProp = clickhouseProp;
    }

    public Integer getQueryTimeOut() {
        return queryTimeOut;
    }

    public void setQueryTimeOut(Integer queryTimeOut) {
        this.queryTimeOut = queryTimeOut;
    }

    public String getSplitKey() {
        return splitKey;
    }

    public void setSplitKey(String splitKey) {
        this.splitKey = splitKey;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getIncreColumn() {
        return increColumn;
    }

    public void setIncreColumn(String increColumn) {
        this.increColumn = increColumn;
    }

    public String getStartLocation() {
        return startLocation;
    }

    public void setStartLocation(String startLocation) {
        this.startLocation = startLocation;
    }

    public Integer getBatchInterval() {
        return batchInterval;
    }

    public void setBatchInterval(Integer batchInterval) {
        this.batchInterval = batchInterval;
    }

    public String getPreSql() {
        return preSql;
    }

    public void setPreSql(String preSql) {
        this.preSql = preSql;
    }

    public String getPostSql() {
        return postSql;
    }

    public void setPostSql(String postSql) {
        this.postSql = postSql;
    }

    @Override
    public String toString() {
        return "ClickhouseConfig{" +
                "url='" + url + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", table='" + table + '\'' +
                ", column=" + column +
                ", clickhouseProp=" + clickhouseProp +
                ", queryTimeOut=" + queryTimeOut +
                ", splitKey='" + splitKey + '\'' +
                ", filter='" + filter + '\'' +
                ", increColumn='" + increColumn + '\'' +
                ", startLocation='" + startLocation + '\'' +
                ", batchInterval=" + batchInterval +
                ", preSql='" + preSql + '\'' +
                ", postSql='" + postSql + '\'' +
                '}';
    }
}
