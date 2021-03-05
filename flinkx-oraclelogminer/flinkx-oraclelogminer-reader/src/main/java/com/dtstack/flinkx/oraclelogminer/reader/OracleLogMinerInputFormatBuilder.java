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
package com.dtstack.flinkx.oraclelogminer.reader;

import com.dtstack.flinkx.config.SpeedConfig;
import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.oraclelogminer.format.LogMinerConfig;
import com.dtstack.flinkx.oraclelogminer.format.LogMinerConnection;
import com.dtstack.flinkx.oraclelogminer.format.OracleLogMinerInputFormat;
import com.dtstack.flinkx.oraclelogminer.util.SqlUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.RetryUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

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

/**
 * @author jiangbo
 * @date 2019/12/16
 */
public class OracleLogMinerInputFormatBuilder extends BaseRichInputFormatBuilder {

    private OracleLogMinerInputFormat format;

    public OracleLogMinerInputFormatBuilder() {
        super.format = format = new OracleLogMinerInputFormat();
    }

    public void setLogMinerConfig(LogMinerConfig logMinerConfig){
        format.logMinerConfig = logMinerConfig;
    }

    @Override
    protected void checkFormat() {
        LogMinerConfig config = format.logMinerConfig;
        StringBuilder sb = new StringBuilder(256);
        if (StringUtils.isBlank(config.getJdbcUrl())) {
            sb.append("No jdbc URL supplied;\n");
        }else{
            //检测数据源连通性
            TelnetUtil.telnet(config.getJdbcUrl());
        }
        if (StringUtils.isBlank(config.getUsername())) {
            sb.append("No database username supplied;\n");
        }
        if (StringUtils.isBlank(config.getPassword())) {
            sb.append("No database password supplied;\n");
        }

        if(sb.length() > 0){
            //JDBC URL、username、password其中之一未配置，直接先抛出异常
            throw new IllegalArgumentException(sb.toString());
        }

        if(config.getFetchSize() < 1){
            sb.append("fetchSize must bigger than 0;\n");
        }
        List<String> list = Arrays.asList(LogMinerConnection.ReadPosition.ALL.name(),
                LogMinerConnection.ReadPosition.CURRENT.name(),
                LogMinerConnection.ReadPosition.TIME.name(),
                LogMinerConnection.ReadPosition.SCN.name());
        if(StringUtils.isBlank(config.getReadPosition())
                || !list.contains(config.getReadPosition().toUpperCase(Locale.ENGLISH))){
            sb.append("readPosition must be one of [all, current, time, scn], current readPosition is [")
                    .append(config.getReadPosition())
                    .append("];\n");
        }
        if (LogMinerConnection.ReadPosition.TIME.name().equalsIgnoreCase(config.getReadPosition())
                && config.getStartTime() == 0){
            sb.append("[startTime] must be supplied when readPosition is [time];\n");
        }

        //校验logMiner cat
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


        SpeedConfig speed = format.getDataTransferConfig().getJob().getSetting().getSpeed();

        if(speed.getReaderChannel() > 1){
            sb.append("logMiner can not support readerChannel bigger than 1, current readerChannel is [")
                    .append(speed.getReaderChannel())
                    .append("];\n");
        }else if(speed.getChannel() > 1){
            sb.append("logMiner can not support channel bigger than 1, current channel is [")
                    .append(speed.getChannel())
                    .append("];\n");
        }

        ClassUtil.forName(config.getDriverName(), getClass().getClassLoader());
        try(
            Connection connection = RetryUtil.executeWithRetry(
                    () -> DriverManager.getConnection(
                            config.getJdbcUrl(),
                            config.getUsername(),
                            config.getPassword()),
                    LogMinerConnection.RETRY_TIMES,
                    LogMinerConnection.SLEEP_TIME,
                    false);
            Statement statement = connection.createStatement();
        ) {
            int oracleVersion = connection.getMetaData().getDatabaseMajorVersion();
            LOG.info("current Oracle version is：{}", oracleVersion);

            //1、校验Oracle账号用户角色组
            ResultSet rs = statement.executeQuery(SqlUtil.SQL_QUERY_ROLES);
            List<String> roles = new ArrayList<>();
            while (rs.next()) {
                String role = rs.getString("GRANTED_ROLE");
                if (StringUtils.isNotEmpty(role)) {
                    roles.add(role.toUpperCase());
                }
            }
            //非DBA角色且非EXECUTE_CATALOG_ROLE角色
            if (!roles.contains("DBA") && !roles.contains("EXECUTE_CATALOG_ROLE")) {
                sb.append("Non-DBA role users must be [EXECUTE_CATALOG_ROLE] role, ")
                        .append("current roles are: [")
                        .append(GsonUtil.GSON.toJson(roles))
                        .append("], please execute sql for empowerment：GRANT EXECUTE_CATALOG_ROLE TO ")
                        .append(config.getUsername())
                        .append(";\n");
            }

            //2、校验Oracle账号权限
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
            //Oracle 11
            if (oracleVersion <= 11) {
                privilegeList = SqlUtil.ORACLE_11_PRIVILEGES_NEEDED;
            } else {
                privilegeList = SqlUtil.PRIVILEGES_NEEDED;
            }
            for (String privilege : privilegeList) {
                if (privileges.contains(privilege)) {
                    privilegeCount++;
                }
            }

            if(privilegeCount != privilegeList.size()){
                if (oracleVersion <= 11) {
                    sb.append("Insufficient permissions, ")
                            .append("current permissions are :")
                            .append(GsonUtil.GSON.toJson(privileges))
                            .append(",please execute sql for empowerment：grant create session, execute_catalog_role, select any transaction, flashback any table, select any table, lock any table, select any dictionary to ")
                            .append(config.getUsername())
                            .append(";\n");
                }else{
                    sb.append("Insufficient permissions, ")
                            .append("current permissions are :")
                            .append(GsonUtil.GSON.toJson(privilegeList))
                            .append(",please execute sql for empowerment：grant create session, execute_catalog_role, select any transaction, flashback any table, select any table, lock any table, logmining, select any dictionary to ")
                            .append(config.getUsername())
                            .append(";\n");
                }
            }

            //3、检查Oracle数据库是否开启日志归档
            rs = statement.executeQuery(SqlUtil.SQL_QUERY_LOG_MODE);
            rs.next();
            String logMode = rs.getString(1);
            if(!"ARCHIVELOG".equalsIgnoreCase(logMode)){
                sb.append("oracle logMode is ")
                        .append(logMode)
                        .append(", please enable log archiving;\n");
            }

            //4、检查Oracle数据库是否开启ALL追加日志
            rs = statement.executeQuery(SqlUtil.SQL_QUERY_SUPPLEMENTAL_LOG_DATA_ALL);
            rs.next();
            if(!"YES".equalsIgnoreCase(rs.getString(1))){
                sb.append("supplemental_log_data_all is not enabled, please execute sql to enable this config: alter database add supplemental log data (all) columns;\n");
            }

            rs.close();

            if(sb.length() > 0){
                throw new IllegalArgumentException(sb.toString());
            }
        }catch (SQLException e){

            StringBuilder detailsInfo = new StringBuilder(sb.length() + 128);

            if(sb.length() > 0){
                detailsInfo.append(" logMiner config not right，details is  ").append(sb.toString());
            }

            detailsInfo.append(" \n error to check logMiner config, e = " ).append(ExceptionUtil.getErrorMessage(e));

            throw new RuntimeException(detailsInfo.toString(), e);
        }
    }
}
