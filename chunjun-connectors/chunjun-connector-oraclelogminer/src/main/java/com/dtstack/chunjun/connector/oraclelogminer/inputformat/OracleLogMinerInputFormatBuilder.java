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
package com.dtstack.chunjun.connector.oraclelogminer.inputformat;

import com.dtstack.chunjun.connector.oraclelogminer.config.LogMinerConfig;
import com.dtstack.chunjun.connector.oraclelogminer.entity.OracleInfo;
import com.dtstack.chunjun.connector.oraclelogminer.listener.LogMinerConnection;
import com.dtstack.chunjun.connector.oraclelogminer.util.SqlUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.RetryUtil;
import com.dtstack.chunjun.util.TelnetUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

@Slf4j
public class OracleLogMinerInputFormatBuilder
        extends BaseRichInputFormatBuilder<OracleLogMinerInputFormat> {

    public OracleLogMinerInputFormatBuilder() {
        super(new OracleLogMinerInputFormat());
    }

    public void setLogMinerConfig(LogMinerConfig logMinerConfig) {
        super.setConfig(logMinerConfig);
        format.logMinerConfig = logMinerConfig;
    }

    public void setRowConverter(AbstractCDCRawTypeMapper rowConverter) {
        this.format.setRowConverter(rowConverter);
    }

    @Override
    protected void checkFormat() {
        LogMinerConfig config = format.logMinerConfig;
        StringBuilder sb = new StringBuilder(256);
        if (StringUtils.isBlank(config.getJdbcUrl())) {
            sb.append("No jdbc URL supplied;\n");
        } else {
            // 检测数据源连通性
            TelnetUtil.telnet(config.getJdbcUrl());
        }
        if (StringUtils.isBlank(config.getUsername())) {
            sb.append("No database username supplied;\n");
        }
        if (StringUtils.isBlank(config.getPassword())) {
            sb.append("No database password supplied;\n");
        }

        if (sb.length() > 0) {
            // JDBC URL、username、password其中之一未配置，直接先抛出异常
            throw new IllegalArgumentException(sb.toString());
        }

        if (config.getFetchSize() < 1) {
            sb.append("fetchSize must bigger than 0;\n");
        }

        if (config.getTransactionEventSize() <= 1) {
            sb.append("transactionEventSize must bigger than 1;\n");
        }

        if (config.getTransactionCacheNumSize() <= 1) {
            sb.append("transactionCacheNumSize must bigger than 1;\n");
        }

        if (config.isPavingData() && config.isSplit()) {
            throw new IllegalArgumentException("can't use pavingData and split at the same time");
        }

        List<String> list =
                Arrays.asList(
                        LogMinerConnection.ReadPosition.ALL.name(),
                        LogMinerConnection.ReadPosition.CURRENT.name(),
                        LogMinerConnection.ReadPosition.TIME.name(),
                        LogMinerConnection.ReadPosition.SCN.name());
        if (StringUtils.isBlank(config.getReadPosition())
                || !list.contains(config.getReadPosition().toUpperCase(Locale.ENGLISH))) {
            sb.append(
                            "readPosition must be one of [all, current, time, scn], current readPosition is [")
                    .append(config.getReadPosition())
                    .append("];\n");
        }
        if (LogMinerConnection.ReadPosition.TIME.name().equalsIgnoreCase(config.getReadPosition())
                && config.getStartTime() == 0) {
            sb.append("[startTime] must be supplied when readPosition is [time];\n");
        }

        // 校验logMiner cat
        if (StringUtils.isNotEmpty(config.getCat())) {
            HashSet<String> set = Sets.newHashSet("INSERT", "UPDATE", "DELETE");
            List<String> cats = Lists.newArrayList(config.getCat().toUpperCase().split(","));
            cats.removeIf(s -> set.contains(s.toUpperCase(Locale.ENGLISH)));
            if (CollectionUtils.isNotEmpty(cats)) {
                sb.append("logMiner cat not support-> ")
                        .append(GsonUtil.GSON.toJson(cats))
                        .append(",just support->")
                        .append(GsonUtil.GSON.toJson(set))
                        .append(";\n");
            }
        }

        LogMinerConfig logMinerConfig = format.logMinerConfig;

        if (logMinerConfig.getParallelism() > 1) {
            sb.append(
                            "logMiner can not support readerChannel bigger than 1, current readerChannel is [")
                    .append(logMinerConfig.getParallelism())
                    .append("];\n");
        }

        ClassUtil.forName(config.getDriverName(), getClass().getClassLoader());
        try (Connection connection =
                        RetryUtil.executeWithRetry(
                                () ->
                                        DriverManager.getConnection(
                                                config.getJdbcUrl(),
                                                config.getUsername(),
                                                config.getPassword()),
                                LogMinerConnection.RETRY_TIMES,
                                LogMinerConnection.SLEEP_TIME,
                                false);
                Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(config.getQueryTimeout().intValue());

            OracleInfo oracleInfo = LogMinerConnection.getOracleInfo(connection);

            checkOracleInfo(oracleInfo, sb);

            if (sb.length() > 0) {
                throw new IllegalArgumentException(sb.toString());
            }

            if (StringUtils.isNotBlank(config.getListenerTables())) {
                checkTableFormat(sb, config.getListenerTables(), oracleInfo.isCdbMode());
            }

            // cdb模式 需要切换到根容器
            if (oracleInfo.isCdbMode()) {
                try {
                    statement.execute(
                            String.format(
                                    SqlUtil.SQL_ALTER_SESSION_CONTAINER,
                                    LogMinerConnection.CDB_CONTAINER_ROOT));
                } catch (SQLException e) {
                    log.warn(
                            "alter session container to CDB$ROOT error,errorInfo is {} ",
                            ExceptionUtil.getErrorMessage(e));
                    sb.append("your account can't alter session container to CDB$ROOT \n");
                }
                if (sb.length() > 0) {
                    throw new IllegalArgumentException(sb.toString());
                }
            }

            // 1、校验Oracle账号用户角色组
            ResultSet rs = statement.executeQuery(SqlUtil.SQL_QUERY_ROLES);
            List<String> roles = new ArrayList<>();
            while (rs.next()) {
                String role = rs.getString("GRANTED_ROLE");
                if (StringUtils.isNotEmpty(role)) {
                    roles.add(role.toUpperCase());
                }
            }
            // 非DBA角色且非EXECUTE_CATALOG_ROLE角色
            if (!roles.contains("DBA") && !roles.contains("EXECUTE_CATALOG_ROLE")) {
                sb.append("Non-DBA role users must be [EXECUTE_CATALOG_ROLE] role, ")
                        .append("current roles are: [")
                        .append(GsonUtil.GSON.toJson(roles))
                        .append(
                                "], please execute sql for empowerment：GRANT EXECUTE_CATALOG_ROLE TO ")
                        .append(config.getUsername())
                        .append(";\n");
            }

            // 2、校验Oracle账号权限
            rs = statement.executeQuery(SqlUtil.SQL_QUERY_PRIVILEGES);
            List<String> privileges = new ArrayList<>();
            while (rs.next()) {
                String privilege = rs.getString("PRIVILEGE");
                if (StringUtils.isNotEmpty(privilege)) {
                    privileges.add(privilege.toUpperCase());
                }
            }

            int privilegeCount = 0;
            List<String> privilegeList;
            // Oracle 11
            if (oracleInfo.getVersion() <= 11) {
                privilegeList = SqlUtil.ORACLE_11_PRIVILEGES_NEEDED;
            } else {
                privilegeList = SqlUtil.PRIVILEGES_NEEDED;
            }
            for (String privilege : privilegeList) {
                if (privileges.contains(privilege)) {
                    privilegeCount++;
                }
            }

            if (privilegeCount != privilegeList.size()) {
                if (oracleInfo.getVersion() <= 11) {
                    sb.append("Insufficient permissions, ")
                            .append("current permissions are :")
                            .append(GsonUtil.GSON.toJson(privileges))
                            .append(
                                    ",please execute sql for empowerment：grant create session, execute_catalog_role, select any transaction, flashback any table, select any table, lock any table, select any dictionary to ")
                            .append(config.getUsername())
                            .append(";\n");
                } else {
                    sb.append("Insufficient permissions, ")
                            .append("current permissions are :")
                            .append(GsonUtil.GSON.toJson(privilegeList))
                            .append(
                                    ",please execute sql for empowerment：grant create session, execute_catalog_role, select any transaction, flashback any table, select any table, lock any table, logmining, select any dictionary to ")
                            .append(config.getUsername())
                            .append(";\n");
                }
            }

            // 3、检查Oracle数据库是否开启日志归档
            rs = statement.executeQuery(SqlUtil.SQL_QUERY_LOG_MODE);
            rs.next();
            String logMode = rs.getString(1);
            if (!"ARCHIVELOG".equalsIgnoreCase(logMode)) {
                sb.append("oracle logMode is ")
                        .append(logMode)
                        .append(", please enable log archiving;\n");
            }

            // 4、检查Oracle数据库是否开启ALL追加日志
            rs = statement.executeQuery(SqlUtil.SQL_QUERY_SUPPLEMENTAL_LOG_DATA_ALL);
            rs.next();
            if (!"YES".equalsIgnoreCase(rs.getString(1))) {
                sb.append(
                        "supplemental_log_data_all is not enabled, please execute sql to enable this config: alter database add supplemental log data (all) columns;\n");
            }

            rs.close();

            if (format.logMinerConfig.getIoThreads() > 3) {
                sb.append("logMinerConfig param ioThreads must less than " + 3);
            }

            if (sb.length() > 0) {
                throw new IllegalArgumentException(sb.toString());
            }
        } catch (SQLException e) {

            StringBuilder detailsInfo = new StringBuilder(sb.length() + 128);

            if (sb.length() > 0) {
                detailsInfo.append(" logMiner config not right，details is  ").append(sb);
            }

            detailsInfo
                    .append(" \n error to check logMiner config, e = ")
                    .append(ExceptionUtil.getErrorMessage(e));

            throw new RuntimeException(detailsInfo.toString(), e);
        }
    }

    private void checkOracleInfo(OracleInfo oracleInfo, StringBuilder sb) {

        // 10以下数据源不支持
        if (oracleInfo.getVersion() < 10) {
            sb.append("we not support ")
                    .append(oracleInfo.getVersion())
                    .append(". we only support versions greater than or equal to oracle10 \n");
        }
    }

    /** 检查监听表格式 pdb.schema.table 或者 schema.table */
    private void checkTableFormat(StringBuilder sb, String listenerTables, boolean isCdb)
            throws SQLException {
        String[] tableWithPdbs = listenerTables.split(ConstantValue.COMMA_SYMBOL);

        for (String tableWithPdb : tableWithPdbs) {
            String[] tables = tableWithPdb.split("\\.");
            // 格式是pdb.schema.table 或者schema.table
            if (tables.length != 2 && tables.length != 3) {
                if (isCdb) {
                    sb.append("The monitored table ")
                            .append(tableWithPdb)
                            .append(
                                    " does not conform to the specification.，The correct format is pdbName.schema.table \n ");
                } else {
                    sb.append("The monitored table ")
                            .append(tableWithPdb)
                            .append(
                                    " does not conform to the specification.，The correct format is schema.table \n ");
                }
            }
        }
    }
}
