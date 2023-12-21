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
package com.dtstack.chunjun.connector.jdbc.config;

import com.dtstack.chunjun.config.CommonConfig;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@EqualsAndHashCode(callSuper = true)
@Data
public class JdbcConfig extends CommonConfig implements Serializable {

    private static final long serialVersionUID = 7543830865792973979L;

    protected List<String> fullColumn;
    /** for postgresql */
    protected String insertSqlMode;

    protected String fieldDelim;
    protected String nullDelim;

    /** for sqlserver */
    protected boolean withNoLock;
    // common
    protected Properties properties;
    // reader
    protected String username;
    protected String password;
    protected List<ConnectionConfig> connection;
    protected String where;
    protected String customSql;
    protected String orderByColumn;
    protected String querySql;
    protected String splitPk;
    protected String splitStrategy;
    protected int fetchSize = 0;
    protected int queryTimeOut = 0;
    // 连接超时时间
    protected int connectTimeOut = 0;
    /** 是否为增量任务 */
    protected boolean increment = false;
    /** 是否为增量轮询 */
    protected boolean polling = false;

    /**
     * Whether to take the maximum value of incrementColumn in db as startLocation in polling mode
     */
    protected boolean pollingFromMax = false;

    /** 字段名称 */
    protected String increColumn;
    /** Whether an OrderBy sort is required,increment mode need set to true. */
    protected boolean isOrderBy = true;
    /** 字段索引 */
    protected int increColumnIndex = -1;
    /** 字段类型 */
    protected String increColumnType;
    /** 字段初始值 */
    protected String startLocation;
    /** 轮询时间间隔 */
    protected long pollingInterval = 5000;
    /** restore字段名称 */
    protected String restoreColumn;
    /** restore字段类型 */
    protected String restoreColumnType;
    /** restore字段索引 */
    protected int restoreColumnIndex = -1;
    /** 用于标记是否保存endLocation位置的一条或多条数据 true：不保存 false(默认)：保存 某些情况下可能出现最后几条数据被重复记录的情况，可以将此参数配置为true */
    protected boolean useMaxFunc = false;
    // writer

    /** 增量同步或者间隔轮询时，是否初始化外部存储 */
    protected Boolean initReporter = true;

    @SerializedName(value = "mode", alternate = "writeMode")
    protected String mode = "INSERT";

    protected List<String> preSql;
    protected List<String> postSql;
    protected List<String> uniqueKey = new ArrayList<>();

    /** upsert 写数据库时，是否null覆盖原来的值 */
    protected boolean allReplace = false;

    protected boolean isAutoCommit = false;

    private boolean defineColumnTypeForStatement = false;

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
}
