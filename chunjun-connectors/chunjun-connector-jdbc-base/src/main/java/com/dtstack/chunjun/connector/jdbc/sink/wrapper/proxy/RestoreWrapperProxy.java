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
package com.dtstack.chunjun.connector.jdbc.sink.wrapper.proxy;

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.wrapper.StatementWrapperUtil;
import com.dtstack.chunjun.constants.CDCConstantValue;
import com.dtstack.chunjun.element.ColumnRowData;

import org.apache.flink.table.data.RowData;

import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * build prepare proxy, proxy implements FieldNamedPreparedStatement. it support to build
 * prepareStmt and manager it with cache.
 */
public class RestoreWrapperProxy extends CachedWrapperProxy {

    protected JdbcDialect jdbcDialect;
    protected JdbcConfig jdbcConfig;

    /** 是否将框架额外添加的扩展信息写入到数据库,默认不写入* */
    protected boolean writeExtInfo;

    public RestoreWrapperProxy(
            Connection connection, JdbcDialect jdbcDialect, boolean writeExtInfo) {
        super(connection, 100, 10, true);
        this.jdbcDialect = jdbcDialect;
        this.writeExtInfo = writeExtInfo;
    }

    @Override
    protected RowData switchExecutorFromCache(RowData record) throws ExecutionException {
        record = ((ColumnRowData) record).sortColumnRowData();
        ColumnRowData columnRowData = (ColumnRowData) record;
        Map<String, Integer> head = columnRowData.getHeaderInfo();
        if (MapUtils.isEmpty(head)) {
            return record;
        }
        int dataBaseIndex = head.get(CDCConstantValue.SCHEMA);
        int tableIndex = head.get(CDCConstantValue.TABLE);

        String database = record.getString(dataBaseIndex).toString();
        String tableName = record.getString(tableIndex).toString();
        String key = getExecutorCacheKey(database, tableName);

        if (!writeExtInfo) {
            columnRowData.removeExtHeaderInfo();
        }

        currentExecutor =
                cache.get(
                        key,
                        () ->
                                StatementWrapperUtil.buildInsertDeleteExecutor(
                                        connection,
                                        jdbcDialect,
                                        database,
                                        tableName,
                                        columnRowData));
        return record;
    }

    public String getExecutorCacheKey(String schema, String table) {
        return String.format("%s_%s", schema, table);
    }
}
