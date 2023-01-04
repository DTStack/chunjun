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

package com.dtstack.chunjun.connector.jdbc.lookup;

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.RowType;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A lookup function for jdbc. */
@Slf4j
public class JdbcAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = 6804288488095569311L;

    protected final JdbcDialect jdbcDialect;
    private final JdbcConfig jdbcConfig;
    private final String query;

    public JdbcAllTableFunction(
            JdbcConfig jdbcConfig,
            JdbcDialect jdbcDialect,
            LookupConfig lookupConfig,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        super(fieldNames, keyNames, lookupConfig, jdbcDialect.getRowConverter(rowType));
        this.jdbcConfig = jdbcConfig;
        this.query =
                jdbcDialect.getSelectFromStatement(
                        jdbcConfig.getSchema(), jdbcConfig.getTable(), fieldNames, new String[] {});
        this.jdbcDialect = jdbcDialect;
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache =
                (Map<String, List<Map<String, Object>>>) cacheRef;
        Connection connection = null;

        try {
            connection = JdbcUtil.getConnection(jdbcConfig, jdbcDialect);
            queryAndFillData(tmpCache, connection);
        } catch (Exception e) {
            log.error("", e);
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.error("", e);
                }
            }
        }
    }

    protected void queryAndFillData(
            Map<String, List<Map<String, Object>>> tmpCache, Connection connection)
            throws SQLException {
        // load data from table
        Statement statement = connection.createStatement();
        statement.setFetchSize(lookupConfig.getFetchSize());
        ResultSet resultSet = statement.executeQuery(query);

        while (resultSet.next()) {
            Map<String, Object> oneRow = new HashMap<>();
            // 防止一条数据有问题，后面数据无法加载
            try {
                GenericRowData rowData = (GenericRowData) rowConverter.toInternal(resultSet);
                for (int i = 0; i < fieldsName.length; i++) {
                    Object object = rowData.getField(i);
                    oneRow.put(fieldsName[i].trim(), object);
                }
                buildCache(oneRow, tmpCache);
            } catch (Exception e) {
                log.error("", e);
            }
        }
    }
}
