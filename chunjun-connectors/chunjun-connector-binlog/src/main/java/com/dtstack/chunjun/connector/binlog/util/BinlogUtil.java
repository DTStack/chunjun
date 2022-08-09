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

import com.dtstack.chunjun.connector.binlog.conf.BinlogConf;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.ChunJunException;

import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Date: 2019/12/03 Company: www.dtstack.com
 *
 * @author tudou
 */
public class BinlogUtil {
    public static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
    public static final int RETRY_TIMES = 3;
    public static final int SLEEP_TIME = 2000;
    private static final Logger LOG = LoggerFactory.getLogger(BinlogUtil.class);
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
            LOG.error(
                    String.format(
                            "error to query BINLOG is enabled , sql = %s", CHECK_BINLOG_ENABLE),
                    e);
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
            LOG.error(
                    String.format("error to query binLog format, sql = %s", CHECK_BINLOG_FORMAT),
                    e);
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
            LOG.error(
                    "'show master status' has an error!,please check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation",
                    e);
            return false;
        }
        return true;
    }

    /**
     * 校验MySQL表权限
     *
     * @param connection MySQL connection
     * @param database database名称
     * @param filter 过滤字符串
     * @param tables 表名
     * @return 没有权限的表
     * @throws SQLException
     */
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

    /**
     * @param schema 需要校验权限的schemaName
     * @param tables 需要校验权限的tableName schemaName权限验证 取schemaName下第一个表进行验证判断整个schemaName下是否具有权限
     */
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
            LOG.error(
                    String.format(
                            "error to check table select privilege error, sql = %s",
                            AUTHORITY_TEMPLATE),
                    sqlException);
            throw sqlException;
        }
    }

    /**
     * 从JDBC URL中获取database名称
     *
     * @param jdbcUrl
     * @return
     */
    public static String getDataBaseByUrl(String jdbcUrl) {
        int paramStartIndex = jdbcUrl.lastIndexOf('?');
        paramStartIndex = paramStartIndex < 0 ? jdbcUrl.length() : paramStartIndex;
        int dbStartIndex = StringUtils.substring(jdbcUrl, 0, paramStartIndex).lastIndexOf('/') + 1;
        return StringUtils.substring(jdbcUrl, dbStartIndex, paramStartIndex);
    }

    /**
     * 每一个表格式化为schema.tableName格式
     *
     * @param schemaName
     * @param tableName
     * @return
     */
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

    public static String formatUpdrdbUsername(String username, String group) {
        StringBuilder stringBuilder = new StringBuilder();
        return stringBuilder.append(username).append("@").append(group).toString();
    }

    /**
     * 判断是否是Updrdb 如果是的话，获取数据节点GROUP信息和表的引擎信息
     *
     * @param connection 连接updrdb proxy节点的jdbc连接
     * @param binlogConf 如果是updrdb，用于装载GROUP信息和表引擎信息
     * @throws SQLException
     */
    public static void getUpdrdbMessage(Connection connection, BinlogConf binlogConf)
            throws SQLException, ChunJunException {
        boolean isUpdrdb = checkIfUpdrdbAndGetGroupInfo(connection, binlogConf);
        if (isUpdrdb) {
            binlogConf.setUpdrdb(true);
            checkUpdrdbUserPrivilege(connection);
            getTableEngineInfo(connection, binlogConf);
        }
    }

    /**
     * 测试当前配置的数据库用户是否有infomation_shcema.TABLES这张表的权限
     *
     * @param connection proxy节点的connection
     */
    private static void checkUpdrdbUserPrivilege(Connection connection) throws SQLException {
        try {
            connection.createStatement().executeQuery(CHECK_UPDRDB_PRIVILEGE);
        } catch (SQLException sqlException) {
            LOG.error(
                    String.format(
                            "error to check UPDRDB User Privilege , sql = %s",
                            CHECK_UPDRDB_PRIVILEGE),
                    sqlException);
            throw sqlException;
        }
    }

    /**
     * 根据table/filter配置信息获取表引擎信息
     *
     * @param connection 连接updrdb proxy节点的jdbc连接
     * @param binlogConf 用于装载表引擎信息
     * @throws SQLException
     */
    private static void getTableEngineInfo(Connection connection, BinlogConf binlogConf)
            throws SQLException, ChunJunException {
        String database = getDataBaseByUrl(binlogConf.getJdbcUrl());

        // 按不同配置情况获取表引擎信息
        // 1.配置了表信息
        // 2.表信息和filter信息均未配置
        // 3.只配置了filter信息
        if (CollectionUtils.isNotEmpty(binlogConf.getTable())) {
            // 按照schema信息分批获取表引擎信息
            List<String> tableNameList = binlogConf.getTable();
            Map<String, List<String>> db_tables = getDatabaseTableMap(tableNameList, database);
            for (Map.Entry<String, List<String>> entry : db_tables.entrySet()) {
                getUpdrdbTableInfoWithDatabase(
                        connection, binlogConf, entry.getKey(), entry.getValue(), ".*");
            }
        } else {
            // filter和table都为空，获取url中的database下的所有表
            if (StringUtils.isBlank(binlogConf.getFilter())) {
                binlogConf.setFilter(database + "\\..*");
            }

            getUpdrdbTableInfoWithFilter(connection, binlogConf);
        }
    }

    /**
     * 根据用户输入的表名以.作为切分键切分database和表名，按database分组并以map形式返回
     *
     * @param tableNameList 用户输入的表名
     * @param defaultDatabase 默认database（url中的schema）
     * @return
     */
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

    /**
     * 通过filter获取表引擎信息
     *
     * @param connection 连接updrdb proxy节点的jdbc连接
     * @param binlogConf 用于装载表引擎信息
     * @throws ChunJunException
     * @throws SQLException
     */
    public static void getUpdrdbTableInfoWithFilter(Connection connection, BinlogConf binlogConf)
            throws ChunJunException, SQLException {
        String filter = binlogConf.getFilter();
        String[] filterInfo = checkAndAnalyzeFilter(filter);
        String database = filterInfo[0];
        String tableNameFilter = filterInfo[1];
        getUpdrdbTableInfoWithDatabase(connection, binlogConf, database, null, tableNameFilter);
    }

    /**
     * 判断filter是否合法
     *
     * @param filter 过滤器
     * @return SchemaRegex+TableRegex
     * @throws ChunJunException filter未明确指定shcema，目前设计必须指定schema
     */
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

    /**
     * 通过database，获取该库下的lamost表和innodb表，并分别add到binlogConf的lamostTableNameList和innodbTableNameList
     *
     * @param connection 连接updrdb proxy节点的jdbc连接
     * @param binlogConf 用于装载表引擎信息
     * @param database 当前查询的数据库
     * @param tableNameList 用户配置的表信息
     * @param regex 用户配置的filter
     * @throws SQLException
     */
    public static void getUpdrdbTableInfoWithDatabase(
            Connection connection,
            BinlogConf binlogConf,
            String database,
            List<String> tableNameList,
            String regex)
            throws SQLException {

        // 使用set类型存放tableName，方便后续判断
        Set<String> tableNameSet = new HashSet<>();
        boolean getAll = true;
        if (!(tableNameList == null) && !tableNameList.isEmpty()) {
            getAll = false;
            tableNameSet.addAll(tableNameList);
        }
        // 根据database获取表信息
        String querySql = String.format(GET_TABLE_ENGINE_WITH_SCHEMA, database);
        ResultSet tableResultSet;
        try {
            tableResultSet = connection.createStatement().executeQuery(querySql);
        } catch (SQLException sqlException) {
            LOG.error(
                    String.format(
                            "error to get UPDRDB table engine infomation, sql = %s", querySql),
                    sqlException);
            throw sqlException;
        }

        List<String> innodbTableList = binlogConf.getInnodbTableNameList();
        List<String> lamostTableList = binlogConf.getLamostTableNameList();
        while (tableResultSet.next()) {
            String tableName = tableResultSet.getString(1);
            String engine = tableResultSet.getString(2);
            if (tableName.matches(regex) && (getAll || tableNameSet.contains(tableName))) {
                tableName = formatTableName(database, tableName);
                if (engine.equalsIgnoreCase("innodb")) {
                    innodbTableList.add(tableName);
                } else if (engine.equalsIgnoreCase("lamost")) {
                    lamostTableList.add(tableName);
                }
            }
        }
    }

    /**
     * 通过 drdb show topology判断是否是updrdb 如果是该语句可执行，则为updrdb，获取GROUP信息并返回true 如果该语句抛出{@linkplain
     * MySQLSyntaxErrorException},则不是updrdb，返回false
     *
     * @param connection 连接updrdb proxy节点的jdbc连接
     * @param binlogConf 用于装载Updrdb的GROUP引擎信息
     * @return
     */
    public static boolean checkIfUpdrdbAndGetGroupInfo(Connection connection, BinlogConf binlogConf)
            throws SQLException {
        if (!checkIfUpdrdb(connection)) {
            // 不是Updrdb 直接返回false
            return false;
        }
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(QUERY_UPDRDB_TOPOLOGY);
            Set<String> datanodeGroupSet = new HashSet<>();
            while (resultSet.next()) {
                String type = resultSet.getString("TYPE");
                if (type.equalsIgnoreCase("datanode")) {
                    datanodeGroupSet.add(resultSet.getString("GROUP"));
                }
            }
            datanodeGroupSet.add("coprocessor");
            binlogConf.setDatanodeGroupList(new ArrayList<>(datanodeGroupSet));
            return true;
        } catch (MySQLSyntaxErrorException e) {
            // 无法执行 drdb show topology ，不是updrdb,直接返回false
            return false;
        } catch (SQLException sqlException) {
            LOG.error(
                    String.format(
                            "error to get UPDRDB topology information , sql = %s",
                            QUERY_UPDRDB_TOPOLOGY),
                    sqlException);
            throw sqlException;
        }
    }

    public static boolean checkIfUpdrdb(Connection connection) throws SQLException {
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(QUERY_VERSION);
            if (resultSet.next()) {
                String versionInfo = resultSet.getString(1);
                return versionInfo.contains("drdb");
            } else {
                return false;
            }
        } catch (SQLException sqlException) {
            LOG.error(
                    String.format("error to get version information , sql = %s", QUERY_VERSION),
                    sqlException);
            throw sqlException;
        }
    }
}
