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

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.NullColumn;

import org.apache.flink.table.data.RowData;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class NoKeyDeleteWrapperProxy extends CachedWrapperProxy {

    private final String schema;
    private final String table;
    private final JdbcDialect jdbcDialect;
    private final List<String> columnNameList;
    private final List<TypeConfig> columnTypeList;

    private List<String> nullColumnNameList = new ArrayList<>();

    public NoKeyDeleteWrapperProxy(
            Connection connection,
            String schema,
            String table,
            JdbcDialect jdbcDialect,
            List<String> columnNameList,
            List<TypeConfig> columnTypeList) {
        super(connection, 100, 10, true);
        this.schema = schema;
        this.table = table;
        this.jdbcDialect = jdbcDialect;
        this.columnNameList = columnNameList;
        this.columnTypeList = columnTypeList;
    }

    @Override
    protected RowData switchExecutorFromCache(RowData record) throws ExecutionException {
        String cacheKey = getCacheKey((ColumnRowData) record);
        currentExecutor =
                cache.get(
                        cacheKey,
                        () ->
                                DynamicSimpleDeleteWrapper.buildExecutor(
                                        columnNameList,
                                        columnTypeList,
                                        nullColumnNameList,
                                        schema,
                                        table,
                                        connection,
                                        jdbcDialect));
        return record;
    }

    private String getCacheKey(ColumnRowData columnRowData) {
        nullColumnNameList.clear();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnNameList.size(); i++) {
            if (columnRowData.getField(i) instanceof NullColumn || columnRowData.isNullAt(i)) {
                sb.append(1);
                nullColumnNameList.add(columnNameList.get(i));
            } else {
                sb.append(0);
            }
        }
        return sb.toString();
    }
}
