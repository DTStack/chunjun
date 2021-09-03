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
package com.dtstack.flinkx.connector.phoenix5.util;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.TelnetUtil;

import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.phoenix.query.QueryServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class Phoenix5Util {

    private static final Logger LOG = LoggerFactory.getLogger(Phoenix5Util.class);

    /**
     * 获取 phoenix5 连接(超时10S)
     *
     * @param driverName
     * @param jdbcConf
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String driverName, JdbcConf jdbcConf) {
        final String url = jdbcConf.getJdbcUrl();
        final String username = jdbcConf.getUsername();
        final String password = jdbcConf.getPassword();

        synchronized (ClassUtil.LOCK_STR) {
            DriverManager.setLoginTimeout(10);
            // telnet
            TelnetUtil.telnet(url);

            ClassUtil.forName(driverName, Thread.currentThread().getContextClassLoader());
            Properties properties = jdbcConf.getProperties();
            if (properties == null) {
                properties = new Properties();
            }
            if (StringUtils.isNotBlank(username)) {
                properties.put("user", username);
            }
            if (StringUtils.isNotBlank(password)) {
                properties.put("password", password);
            }
            if (properties.get(QueryServices.MUTATE_BATCH_SIZE_ATTRIB) == null) {
                // 执行过程中被批处理并自动提交的行数
                properties.setProperty(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, "100000");
            }
            if (properties.get(QueryServices.MAX_MUTATION_SIZE_ATTRIB) == null) {
                // 客户端批处理的最大行数
                properties.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "1000000");
            }
            if (properties.get(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB) == null) {
                // 客户端批处理的最大数据量（单位：B）1GB
                properties.setProperty(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB, "1073741824");
            }
            Connection conn;
            try {
                conn = DriverManager.getConnection(url, properties);
            } catch (SQLException e) {
                throw new FlinkxRuntimeException("Unable to get phoenix connection.", e);
            }
            return conn;
        }
    }

    public static Pair<List<String>, List<String>> getTableMetaData(
            List<FieldConf> metaColumns, String tableName, Connection dbConn) {

        Preconditions.checkNotNull(metaColumns, "metaColumns must not be null.");
        Preconditions.checkNotNull(tableName, "tableName must not be null.");
        Preconditions.checkNotNull(dbConn, "phoenix connection must not be null.");

        List<String> columnNameList = new ArrayList<>(metaColumns.size());
        metaColumns.forEach(columnName -> columnNameList.add(columnName.getName()));
        String sql;
        PreparedStatement ps;
        ResultSet resultSet = null;
        ResultSetMetaData meta = null;
        int columnCount;
        try {
            // retrieve once table for obtain metadata(column name and type).
            sql = getSqlWithLimit0(columnNameList, tableName);
            ps = dbConn.prepareStatement(sql);
            resultSet = ps.executeQuery();
            meta = ps.getMetaData();
            columnCount = meta.getColumnCount();
            List<String> fullColumnNameList = new ArrayList<>(columnCount);
            List<String> fullColumnTypeList = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                String name = meta.getColumnName(i);
                String type = meta.getColumnTypeName(i);
                fullColumnNameList.add(name);
                fullColumnTypeList.add(type);
                LOG.info("field count, name = {}, type = {}", i + "," + name, type);
            }
            return Pair.of(fullColumnNameList, fullColumnTypeList);
        } catch (SQLException e) {
            throw new FlinkxRuntimeException(
                    String.format(
                            "error to get meta from [%s.%s]",
                            meta != null ? meta.toString() : "meta is null", tableName),
                    e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    throw new FlinkxRuntimeException(
                            String.format("close connection fail  [%s]", tableName), e);
                }
            }
        }
    }

    public static String getSqlWithLimit0(List<String> metaColumns, String table) {
        String columnStr;
        if (metaColumns.size() == 1 && ConstantValue.STAR_SYMBOL.equals(metaColumns.get(0))) {
            columnStr = ConstantValue.STAR_SYMBOL;
        } else {
            columnStr = quoteColumns(metaColumns, null);
        }
        return new StringBuilder(256)
                .append("SELECT ")
                .append(columnStr)
                .append(" FROM ")
                .append(quoteTable(table))
                .append(" LIMIT 0")
                .toString();
    }

    public static String quoteTable(String table) {
        String[] parts = table.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; ++i) {
            if (i != 0) {
                sb.append(".");
            }
            sb.append(parts[i]);
        }
        return sb.toString();
    }

    public static String quoteColumns(List<String> column, String table) {
        String prefix = StringUtils.isBlank(table) ? "" : quoteTable(table) + ".";
        List<String> list = new ArrayList<>();
        for (String col : column) {
            list.add(prefix + quoteColumn(col));
        }
        return StringUtils.join(list, ",");
    }

    public static String quoteColumn(String column) {
        return column;
    }

    public static void closeDbResources(ResultSet rs, Statement stmt, Connection conn) {
        try {
            if (null != rs) {
                rs.close();
            }

            if (null != stmt) {
                stmt.close();
            }

            if (null != conn) {
                conn.close();
            }
        } catch (Throwable t) {
            LOG.warn("", t);
        }
    }
}
