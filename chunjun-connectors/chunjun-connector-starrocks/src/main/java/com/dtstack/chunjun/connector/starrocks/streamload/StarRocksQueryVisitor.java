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

package com.dtstack.chunjun.connector.starrocks.streamload;

import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;
import com.dtstack.chunjun.connector.starrocks.connection.StarRocksJdbcConnectionOptions;
import com.dtstack.chunjun.connector.starrocks.connection.StarRocksJdbcConnectionProvider;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class StarRocksQueryVisitor implements Serializable {

    private static final long serialVersionUID = -6104517100465696782L;

    private final StarRocksJdbcConnectionProvider jdbcConnProvider;
    private final String database;
    private final String table;

    public StarRocksQueryVisitor(StarRocksConfig starRocksConfig) {
        StarRocksJdbcConnectionOptions jdbcOptions =
                new StarRocksJdbcConnectionOptions(
                        starRocksConfig.getUrl(),
                        starRocksConfig.getUsername(),
                        starRocksConfig.getPassword());
        this.jdbcConnProvider = new StarRocksJdbcConnectionProvider(jdbcOptions);
        this.database = starRocksConfig.getDatabase();
        this.table = starRocksConfig.getTable();
    }

    public StarRocksJdbcConnectionProvider getJdbcConnProvider() {
        return jdbcConnProvider;
    }

    public List<Map<String, Object>> getTableColumnsMetaData() {
        return getTableColumnsMetaData(database, table);
    }

    public List<Map<String, Object>> getTableColumnsMetaData(String database, String table) {
        final String query =
                "select `COLUMN_NAME`, `COLUMN_KEY`, `DATA_TYPE`, `COLUMN_SIZE`, `DECIMAL_DIGITS` from `information_schema`.`COLUMNS` where `TABLE_SCHEMA`=? and `TABLE_NAME`=?;";
        List<Map<String, Object>> rows;
        try {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Executing query '%s'", query));
            }
            rows = executeQuery(query, database, table);
        } catch (ClassNotFoundException se) {
            throw new IllegalArgumentException("Failed to find jdbc driver." + se.getMessage(), se);
        } catch (SQLException se) {
            throw new IllegalArgumentException(
                    "Failed to get table schema info from StarRocks. " + se.getMessage(), se);
        }
        return rows;
    }

    public String getStarRocksVersion() {
        final String query = "select current_version() as ver;";
        List<Map<String, Object>> rows;
        try {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Executing query '%s'", query));
            }
            rows = executeQuery(query);
            if (rows.isEmpty()) {
                return "";
            }
            String version = rows.get(0).get("ver").toString();
            log.info(String.format("StarRocks version: [%s].", version));
            return version;
        } catch (ClassNotFoundException se) {
            throw new IllegalArgumentException("Failed to find jdbc driver." + se.getMessage(), se);
        } catch (SQLException se) {
            throw new IllegalArgumentException(
                    "Failed to get StarRocks version. " + se.getMessage(), se);
        }
    }

    private List<Map<String, Object>> executeQuery(String query, String... args)
            throws ClassNotFoundException, SQLException {
        jdbcConnProvider.checkValid();
        PreparedStatement stmt =
                jdbcConnProvider
                        .getConnection()
                        .prepareStatement(
                                query,
                                ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_READ_ONLY);
        for (int i = 0; i < args.length; i++) {
            stmt.setString(i + 1, args[i]);
        }
        ResultSet rs = stmt.executeQuery();
        rs.next();
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();
        List<Map<String, Object>> list = new ArrayList<>();
        int currRowIndex = rs.getRow();
        rs.beforeFirst();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>(columns);
            for (int i = 1; i <= columns; ++i) {
                row.put(meta.getColumnName(i), rs.getObject(i));
            }
            list.add(row);
        }
        rs.absolute(currRowIndex);
        rs.close();
        jdbcConnProvider.close();
        return list;
    }

    public void close() {
        jdbcConnProvider.close();
    }

    public boolean hasPartitions(String database, String table) {
        ResultSet rs;
        try {
            jdbcConnProvider.checkValid();
            final String query = "SHOW PARTITIONS FROM " + database + "." + table + ";";
            PreparedStatement stmt = jdbcConnProvider.getConnection().prepareStatement(query);
            rs = stmt.executeQuery();
            return rs.next();
        } catch (ClassNotFoundException se) {
            throw new IllegalArgumentException("Failed to find jdbc driver." + se.getMessage(), se);
        } catch (SQLException se) {
            throw new IllegalArgumentException(
                    "Failed to get table partition info from StarRocks. " + se.getMessage(), se);
        } finally {
            jdbcConnProvider.close();
        }
    }
}
