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
package com.dtstack.flinkx.binlog;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Date: 2019/12/03
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class BinlogUtil {
    private static final Logger LOG = LoggerFactory.getLogger(BinlogUtil.class);

    public static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
    //是否开启binlog
    private static final String CHECK_BINLOG_ENABLE = "show variables where variable_name = 'log_bin';;";
    //查看binlog format
    private static final String CHECK_BINLOG_FORMAT = "show variables where variable_name = 'binlog_format';;";
    //校验用户是否有权限
    private static final String CHECK_USER_PRIVILEGE = "show master status ;";


    private static final String AUTHORITY_TEMPLATE = "SELECT count(1) FROM %s LIMIT 1";

    private static final String QUERY_SCHEMA_TABLE_TEMPLATE = "SELECT TABLE_NAME From information_schema.TABLES WHERE TABLE_SCHEMA='%s' LIMIT 1";


    public static final int RETRY_TIMES = 3;

    public static final int SLEEP_TIME = 2000;


    /**
     * 校验是否开启binlog
     *
     * @param conn
     * @return
     * @throws SQLException
     */
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
            LOG.error("error to query BINLOG is enabled , sql = {}, e = {}", CHECK_BINLOG_ENABLE, ExceptionUtil.getErrorMessage(e));
            throw e;
        }
    }

    /**
     * 校验binlog的format格式
     *
     * @param conn
     * @return
     * @throws SQLException
     */
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
            LOG.error("error to query binLog format, sql = {}, e = {}", CHECK_BINLOG_FORMAT, ExceptionUtil.getErrorMessage(e));
            throw e;
        }
    }

    /**
     * 效验用户的权限
     *
     * @param conn
     * @return
     */
    public static boolean checkUserPrivilege(Connection conn) {
        try (Statement statement = conn.createStatement()) {
            statement.execute(CHECK_USER_PRIVILEGE);
        } catch (SQLException e) {
            LOG.error("'show master status' has an error!,please check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation, e = {}", ExceptionUtil.getErrorMessage(e));
            return false;
        }
        return true;
    }

    public static List<String> checkTablesPrivilege(Connection connection, String database, String filter, List<String> tables) throws SQLException {
        if (CollectionUtils.isNotEmpty(tables)) {
            HashMap<String, String> checkedTable = new HashMap<>(tables.size());
            //按照.切割字符串需要转义
            String regexSchemaSplit = "\\" + ConstantValue.POINT_SYMBOL;
            tables.stream()
                    //每一个表格式化为schema.tableName格式
                    .map(t -> formatTableName(database, t))
                    //只需要每个schema下的一个表进行判断
                    .forEach(t -> checkedTable.putIfAbsent(t.split(regexSchemaSplit)[0], t));

            //检验每个schema下的第一个表的权限
            return checkSourceAuthority(connection, null, checkedTable.values());
        } else if (StringUtils.isBlank(filter)) {
            //检验schema下任意一张表的权限
            return checkSourceAuthority(connection, database, null);
        }
        return null;
    }

    /**
     * @param schema 需要校验权限的schemaName
     * @param tables 需要校验权限的tableName
     *               schemaName权限验证 取schemaName下第一个表进行验证判断整个schemaName下是否具有权限
     */
    public static List<String> checkSourceAuthority(Connection connection, String schema, Collection<String> tables) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            //Schema不为空且用户没有指定tables 就获取一张表判断权限
            if (StringUtils.isNotBlank(schema) && CollectionUtils.isEmpty(tables)) {
                try (ResultSet resultSet = statement.executeQuery(String.format(QUERY_SCHEMA_TABLE_TEMPLATE, schema))) {
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
                    //判断用户是否具备tableName下的读权限
                    statement.executeQuery(String.format(AUTHORITY_TEMPLATE, tableName));
                } catch (SQLException e) {
                    failedTables.add(tableName);
                }
            }

            return failedTables;
        } catch (SQLException sqlException) {
            LOG.error("error to check table select privilege error, sql = {}, e = {}", AUTHORITY_TEMPLATE, ExceptionUtil.getErrorMessage(sqlException));
            throw sqlException;
        }
    }


    public static String getDataBaseByUrl(String jdbcUrl) {
        int idx = jdbcUrl.lastIndexOf('?');

        if (idx != -1) {
            return StringUtils.substring(jdbcUrl, jdbcUrl.lastIndexOf('/') + 1, idx);
        } else {
            return StringUtils.substring(jdbcUrl, jdbcUrl.lastIndexOf('/') + 1);
        }
    }

    private static String formatTableName(String schemaName, String tableName) {
        StringBuilder stringBuilder = new StringBuilder();
        if (tableName.contains(ConstantValue.POINT_SYMBOL)) {
            return tableName;
        } else {
            return stringBuilder.append(schemaName).append(ConstantValue.POINT_SYMBOL).append(tableName).toString();
        }
    }
}
