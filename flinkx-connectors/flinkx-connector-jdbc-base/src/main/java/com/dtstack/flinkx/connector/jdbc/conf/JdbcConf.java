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

import com.dtstack.flinkx.conf.FlinkxCommonConf;

import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 2021/04/12 Company: www.dtstack.com
 *
 * @author tudou
 */
public class JdbcConf extends FlinkxCommonConf implements Serializable {
    private static final long serialVersionUID = 1L;

    protected List<String> fullColumn;
    /** for postgresql */
    protected String insertSqlMode;

    protected String fieldDelim;
    protected String nullDelim;

    /** for sqlserver */
    private boolean withNoLock;
    // common
    private Properties properties;
    // reader
    private String username;
    private String password;
    private List<ConnectionConf> connection;
    private String where;
    private String customSql;
    private String orderByColumn;
    private String querySql;
    private String splitPk;
    private String splitStrategy = "range";
    private int fetchSize = 0;
    private int queryTimeOut = 0;
    // 连接超时时间
    private int connectTimeOut = 0;
    /** 是否为增量任务 */
    private boolean increment = false;
    /** 是否为增量轮询 */
    private boolean polling = false;
    /** 字段名称 */
    private String increColumn;
    /** 字段索引 */
    private int increColumnIndex = -1;
    /** 字段类型 */
    private String increColumnType;
    /** 字段初始值 */
    private String startLocation;
    /** 轮询时间间隔 */
    private long pollingInterval = 5000;
    /** restore字段名称 */
    private String restoreColumn;
    /** restore字段类型 */
    private String restoreColumnType;
    /** restore字段索引 */
    private int restoreColumnIndex = -1;
    /** 用于标记是否保存endLocation位置的一条或多条数据 true：不保存 false(默认)：保存 某些情况下可能出现最后几条数据被重复记录的情况，可以将此参数配置为true */
    private boolean useMaxFunc = false;
    // writer

    /** 增量同步或者间隔轮询时，是否初始化外部存储 */
    private Boolean initReporter = true;

    @SerializedName(value = "mode", alternate = "writeMode")
    private String mode = "INSERT";

    private List<String> preSql;
    private List<String> postSql;
    private List<String> uniqueKey;
    @Deprecated private Map<String, List<String>> updateKey;

    /** upsert 写数据库时，是否null覆盖原来的值 */
    private boolean allReplace = false;

    public Boolean getInitReporter() {
        return initReporter;
    }

    public void setInitReporter(Boolean initReporter) {
        this.initReporter = initReporter;
    }

    public String getFieldDelim() {
        return fieldDelim;
    }

    public void setFieldDelim(String fieldDelim) {
        this.fieldDelim = fieldDelim;
    }

    public String getNullDelim() {
        return nullDelim;
    }

    public void setNullDelim(String nullDelim) {
        this.nullDelim = nullDelim;
    }

    public String getTable() {
        if (StringUtils.isNotBlank(getCustomSql())) {
            return null;
        }
        return connection.get(0).getTable().get(0);
    }

    public void setTable(String table) {
        connection.get(0).getTable().set(0, table);
    }

    public String getSchema() {
        return connection.get(0).getSchema();
    }

    public void setSchema(String schema) {
        connection.get(0).setSchema(schema);
    }

    public String getJdbcUrl() {
        return connection.get(0).obtainJdbcUrl();
    }

    public void setJdbcUrl(String url) {
        connection.get(0).putJdbcUrl(url);
    }

    // ------------------- getter、setter -------------------

    public List<String> getFullColumn() {
        return fullColumn;
    }

    public void setFullColumn(List<String> fullColumn) {
        this.fullColumn = fullColumn;
    }

    public String getInsertSqlMode() {
        return insertSqlMode;
    }

    public void setInsertSqlMode(String insertSqlMode) {
        this.insertSqlMode = insertSqlMode;
    }

    public boolean isWithNoLock() {
        return withNoLock;
    }

    public void setWithNoLock(boolean withNoLock) {
        this.withNoLock = withNoLock;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
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

    public List<ConnectionConf> getConnection() {
        return connection;
    }

    public void setConnection(List<ConnectionConf> connection) {
        this.connection = connection;
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

    public String getOrderByColumn() {
        return orderByColumn;
    }

    public void setOrderByColumn(String orderByColumn) {
        this.orderByColumn = orderByColumn;
    }

    public String getQuerySql() {
        return querySql;
    }

    public void setQuerySql(String querySql) {
        this.querySql = querySql;
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

    public int getConnectTimeOut() {
        return connectTimeOut;
    }

    public void setConnectTimeOut(int connectTimeOut) {
        this.connectTimeOut = connectTimeOut;
    }

    public boolean isIncrement() {
        return increment;
    }

    public void setIncrement(boolean increment) {
        this.increment = increment;
    }

    public boolean isPolling() {
        return polling;
    }

    public void setPolling(boolean polling) {
        this.polling = polling;
    }

    public String getIncreColumn() {
        return increColumn;
    }

    public void setIncreColumn(String increColumn) {
        this.increColumn = increColumn;
    }

    public int getIncreColumnIndex() {
        return increColumnIndex;
    }

    public void setIncreColumnIndex(int increColumnIndex) {
        this.increColumnIndex = increColumnIndex;
    }

    public String getIncreColumnType() {
        return increColumnType;
    }

    public void setIncreColumnType(String increColumnType) {
        this.increColumnType = increColumnType;
    }

    public String getStartLocation() {
        return startLocation;
    }

    public void setStartLocation(String startLocation) {
        this.startLocation = startLocation;
    }

    public long getPollingInterval() {
        return pollingInterval;
    }

    public void setPollingInterval(long pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    public String getRestoreColumn() {
        return restoreColumn;
    }

    public void setRestoreColumn(String restoreColumn) {
        this.restoreColumn = restoreColumn;
    }

    public String getRestoreColumnType() {
        return restoreColumnType;
    }

    public void setRestoreColumnType(String restoreColumnType) {
        this.restoreColumnType = restoreColumnType;
    }

    public int getRestoreColumnIndex() {
        return restoreColumnIndex;
    }

    public void setRestoreColumnIndex(int restoreColumnIndex) {
        this.restoreColumnIndex = restoreColumnIndex;
    }

    public boolean isUseMaxFunc() {
        return useMaxFunc;
    }

    public void setUseMaxFunc(boolean useMaxFunc) {
        this.useMaxFunc = useMaxFunc;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public List<String> getPreSql() {
        return preSql;
    }

    public void setPreSql(List<String> preSql) {
        this.preSql = preSql;
    }

    public List<String> getPostSql() {
        return postSql;
    }

    public void setPostSql(List<String> postSql) {
        this.postSql = postSql;
    }

    public List<String> getUniqueKey() {
        return uniqueKey;
    }

    public void setUniqueKey(List<String> uniqueKey) {
        this.uniqueKey = uniqueKey;
    }

    public boolean isAllReplace() {
        return allReplace;
    }

    public void setAllReplace(boolean allReplace) {
        this.allReplace = allReplace;
    }

    public String getSplitStrategy() {
        return splitStrategy;
    }

    public void setSplitStrategy(String splitStrategy) {
        this.splitStrategy = splitStrategy;
    }

    public Map<String, List<String>> getUpdateKey() {
        return updateKey;
    }

    @Override
    public String toString() {
        return "JdbcConf{"
                + "fullColumn="
                + fullColumn
                + ", insertSqlMode='"
                + insertSqlMode
                + '\''
                + ", fieldDelim='"
                + fieldDelim
                + '\''
                + ", nullDelim='"
                + nullDelim
                + '\''
                + ", withNoLock="
                + withNoLock
                + ", properties="
                + properties
                + ", username='"
                + username
                + '\''
                + ", password='"
                + password
                + '\''
                + ", connection="
                + connection
                + ", where='"
                + where
                + '\''
                + ", customSql='"
                + customSql
                + '\''
                + ", orderByColumn='"
                + orderByColumn
                + '\''
                + ", querySql='"
                + querySql
                + '\''
                + ", splitPk='"
                + splitPk
                + '\''
                + ", splitStrategy='"
                + splitStrategy
                + '\''
                + ", fetchSize="
                + fetchSize
                + ", queryTimeOut="
                + queryTimeOut
                + ", connectTimeOut="
                + connectTimeOut
                + ", increment="
                + increment
                + ", polling="
                + polling
                + ", increColumn='"
                + increColumn
                + '\''
                + ", increColumnIndex="
                + increColumnIndex
                + ", increColumnType='"
                + increColumnType
                + '\''
                + ", startLocation='"
                + startLocation
                + '\''
                + ", pollingInterval="
                + pollingInterval
                + ", restoreColumn='"
                + restoreColumn
                + '\''
                + ", restoreColumnType='"
                + restoreColumnType
                + '\''
                + ", restoreColumnIndex="
                + restoreColumnIndex
                + ", useMaxFunc="
                + useMaxFunc
                + ", mode='"
                + mode
                + '\''
                + ", preSql="
                + preSql
                + ", postSql="
                + postSql
                + ", uniqueKey="
                + uniqueKey
                + ", updateKey="
                + updateKey
                + ", allReplace="
                + allReplace
                + '}';
    }
}
