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
package com.dtstack.chunjun.connector.binlog.util;

import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.ChunJunException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class BinlogUtil {
    public static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
    public static final int RETRY_TIMES = 3;
    public static final int SLEEP_TIME = 2000;
    // 是否开启binlog
    private static final String CHECK_BINLOG_ENABLE =
            "show variables where variable_name = 'log_bin';";
    // 查看binlog format
    private static final String CHECK_BINLOG_FORMAT =
            "show variables where variable_name = 'binlog_format';";
    // 校验用户是否有权限
    private static final String CHECK_USER_PRIVILEGE = "show master status ;";
    private static final String AUTHORITY_TEMPLATE = "SELECT count(1) FROM %s LIMIT 1";
    private static final String QUERY_SCHEMA_TABLE_TEMPLATE =
            "SELECT TABLE_NAME From information_schema.TABLES WHERE TABLE_SCHEMA='%s' LIMIT 1";

    private static final String QUERY_VERSION = "SELECT VERSION()";
    // 获取Updrdb的topology信息
    private static final String QUERY_UPDRDB_TOPOLOGY = "DRDB SHOW TOPOLOGY";
    // 获取指定数据库下所有表的引擎信息
    private static final String GET_TABLE_ENGINE_WITH_SCHEMA =
            "SELECT TABLE_NAME,ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s'";
    private static final String CHECK_UPDRDB_PRIVILEGE =
            "SELECT ENGINE FROM information_schema.TABLES LIMIT 1";

    public static boolean checkEnabledBinlog(Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(CHECK_BINLOG_ENABLE)) {
                if (rs.next()) {
                    String binLog = rs.getString("Value");
                    if (StringUtils.isNotBlank(binLog)) {
                        return "ON".equalsIgnoreCase(binLog);
                    }
                }
                return false;
            }
        } catch (SQLException e) {
            log.error(
                    String.format(
                            "error to query BINLOG is enabled , sql = %s", CHECK_BINLOG_ENABLE),
                    e);
            throw e;
        }
    }

    public static boolean checkBinlogFormat(Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(CHECK_BINLOG_FORMAT)) {
                if (rs.next()) {
                    String logFormat = rs.getString("Value");
                    if (StringUtils.isNotBlank(logFormat)) {
                        return "row".equalsIgnoreCase(logFormat);
                    }
                }
                return false;
            }
        } catch (SQLException e) {
            log.error(
                    String.format("error to query binLog format, sql = %s", CHECK_BINLOG_FORMAT),
                    e);
            throw e;
        }
    }

    public static boolean checkUserPrivilege(Connection conn) {
        try (Statement statement = conn.createStatement()) {
            statement.execute(CHECK_USER_PRIVILEGE);
        } catch (SQLException e) {
            log.error(
                    "'show master status' has an error!,please check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation",
                    e);
            return false;
        }
        return true;
    }

    public static List<String> checkTablesPrivilege(
            Connection connection, String database, String filter, List<String> tables)
            throws SQLException {
        if (CollectionUtils.isNotEmpty(tables)) {
            HashMap<String, String> checkedTable = new HashMap<>(tables.size());
            // 按照.切割字符串需要转义
            String regexSchemaSplit = "\\" + ConstantValue.POINT_SYMBOL;
            tables.stream()
                    // 每一个表格式化为schema.tableName格式
                    .map(t -> formatTableName(database, t))
                    // 只需要每个schema下的一个表进行判断
                    .forEach(t -> checkedTable.putIfAbsent(t.split(regexSchemaSplit)[0], t));

            // 检验每个schema下的第一个表的权限
            return checkSourceAuthority(connection, null, checkedTable.values());
        } else if (StringUtils.isBlank(filter)) {
            // 检验schema下任意一张表的权限
            return checkSourceAuthority(connection, database, null);
        }
        return null;
    }

    public static List<String> checkSourceAuthority(
            Connection connection, String schema, Collection<String> tables) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            // Schema不为空且用户没有指定tables 就获取一张表判断权限
            if (StringUtils.isNotBlank(schema) && CollectionUtils.isEmpty(tables)) {
                try (ResultSet resultSet =
                        statement.executeQuery(
                                String.format(QUERY_SCHEMA_TABLE_TEMPLATE, schema))) {
                    if (resultSet.next()) {
                        String tableName = resultSet.getString(1);
                        if (StringUtils.isNotBlank(tableName)) {
                            tables = Collections.singletonList(formatTableName(schema, tableName));
                        }
                    }
                }
            }
            if (CollectionUtils.isEmpty(tables)) {
                return null;
            }

            List<String> failedTables = new ArrayList<>(tables.size());
            for (String tableName : tables) {
                try {
                    // 判断用户是否具备tableName下的读权限
                    statement.executeQuery(String.format(AUTHORITY_TEMPLATE, tableName));
                } catch (SQLException e) {
                    failedTables.add(tableName);
                }
            }

            return failedTables;
        } catch (SQLException sqlException) {
            log.error(
                    String.format(
                            "error to check table select privilege error, sql = %s",
                            AUTHORITY_TEMPLATE),
                    sqlException);
            throw sqlException;
        }
    }

    public static String getDataBaseByUrl(String jdbcUrl) {
        int paramStartIndex = jdbcUrl.lastIndexOf('?');
        paramStartIndex = paramStartIndex < 0 ? jdbcUrl.length() : paramStartIndex;
        int dbStartIndex = StringUtils.substring(jdbcUrl, 0, paramStartIndex).lastIndexOf('/') + 1;
        return StringUtils.substring(jdbcUrl, dbStartIndex, paramStartIndex);
    }

    public static String formatTableName(String schemaName, String tableName) {
        StringBuilder stringBuilder = new StringBuilder();
        if (tableName.contains(ConstantValue.POINT_SYMBOL)) {
            return tableName;
        } else {
            return stringBuilder
                    .append(schemaName)
                    .append(ConstantValue.POINT_SYMBOL)
                    .append(tableName)
                    .toString();
        }
    }

    public static Map<String, List<String>> getDatabaseTableMap(
            List<String> tableNameList, String defaultDatabase) {
        Map<String, List<String>> db_tables = new HashMap<>();
        // 按照.切割字符串需要转义
        String regexSchemaSplit = "\\" + ConstantValue.POINT_SYMBOL;
        tableNameList.forEach(
                tableName -> {
                    String[] tableInfo = tableName.split(regexSchemaSplit);
                    if (tableInfo.length == 1) {
                        List<String> curTableNameList =
                                db_tables.getOrDefault(db_tables, new ArrayList<>());
                        curTableNameList.add(tableName);
                        db_tables.put(defaultDatabase, curTableNameList);
                    } else {
                        List<String> curTableNameList =
                                db_tables.getOrDefault(tableInfo[0], new ArrayList<>());
                        curTableNameList.add(tableInfo[1]);
                        db_tables.put(tableInfo[0], curTableNameList);
                    }
                });
        return db_tables;
    }

    public static String[] checkAndAnalyzeFilter(String filter) throws ChunJunException {
        // filter指定了所有表
        if (filter.equals(".*") || filter.equals(".*\\..*")) {
            throw new ChunJunException("drdb binlogReader must specify schema!");
        } else {
            String[] filterInfo = filter.split("\\\\.");
            if (filterInfo.length != 2) {
                throw new ChunJunException(String.format("unsupported regex [%s]", filter));
            }
            // 未明确指定schema
            if (!filterInfo[0].matches("([1-9]|[a-z]|[A-Z])*")) {
                throw new ChunJunException("drdb binlogReader must specify schema!");
            }
            return filterInfo;
        }
    }
}
