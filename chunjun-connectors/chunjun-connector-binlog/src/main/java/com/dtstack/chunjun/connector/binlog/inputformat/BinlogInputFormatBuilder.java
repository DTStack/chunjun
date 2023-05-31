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
package com.dtstack.chunjun.connector.binlog.inputformat;

import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.connector.binlog.config.BinlogConfig;
import com.dtstack.chunjun.connector.binlog.util.BinlogUtil;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.RetryUtil;
import com.dtstack.chunjun.util.TelnetUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

public class BinlogInputFormatBuilder extends BaseRichInputFormatBuilder<BinlogInputFormat> {

    public BinlogInputFormatBuilder() {
        super(new BinlogInputFormat());
    }

    public void setBinlogConf(BinlogConfig binlogConfig) {
        super.setConfig(binlogConfig);
        this.format.setBinlogConfig(binlogConfig);
    }

    public void setRowConverter(AbstractCDCRawTypeMapper rowConverter) {
        setRowConverter(rowConverter, false);
    }

    public void setRowConverter(AbstractCDCRawTypeMapper rowConverter, boolean useAbstractColumn) {
        this.format.setCdcRowConverter(rowConverter);
        format.setUseAbstractColumn(useAbstractColumn);
    }

    @Override
    public BaseRichInputFormat finish() {
        Preconditions.checkNotNull(format);
        if (format.getConfig().isCheckFormat()) {
            checkFormat();
        }
        return format;
    }

    @Override
    protected void checkFormat() {
        StringBuilder sb = new StringBuilder(256);
        BinlogConfig binlogConfig = format.getBinlogConfig();
        if (StringUtils.isBlank(binlogConfig.username)) {
            sb.append("No username supplied;\n");
        }

        if (StringUtils.isBlank(binlogConfig.jdbcUrl)) {
            sb.append("No url supplied;\n");
        } else {
            // 检测数据源连通性
            TelnetUtil.telnet(binlogConfig.jdbcUrl);
        }

        if (StringUtils.isBlank(binlogConfig.host)) {
            sb.append("No host supplied;\n");
        }

        if (StringUtils.isBlank(binlogConfig.cat)) {
            sb.append("No cat supplied;\n");
        }

        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }

        if (binlogConfig.getParallelism() > 1) {
            sb.append("binLog can not support channel bigger than 1, current channel is [")
                    .append(binlogConfig.getParallelism())
                    .append("];\n");
        }

        // 校验binlog cat
        if (StringUtils.isNotEmpty(binlogConfig.getCat())) {
            List<String> cats = Lists.newArrayList(binlogConfig.getCat().toUpperCase().split(","));
            cats.removeIf(s -> EventType.contains(s.toUpperCase(Locale.ENGLISH)));
            if (CollectionUtils.isNotEmpty(cats)) {
                sb.append("binlog cat not support-> ")
                        .append(GsonUtil.GSON.toJson(cats))
                        .append(",just support->")
                        .append(GsonUtil.GSON.toJson(EventType.values()))
                        .append(";\n");
            }
        }

        // 校验binlog的start参数
        if (MapUtils.isNotEmpty(binlogConfig.getStart())) {
            try {
                MapUtils.getLong(binlogConfig.getStart(), "timestamp");
            } catch (Exception e) {
                sb.append(
                                "binlog start parameter of timestamp  must be long type, but your value is -> ")
                        .append(binlogConfig.getStart().get("timestamp"))
                        .append(";\n");
            }
            try {
                MapUtils.getLong(binlogConfig.getStart(), "position");
            } catch (Exception e) {
                sb.append(
                                "binlog start parameter of position  must be long type, but your value is -> ")
                        .append(binlogConfig.getStart().get("timestamp"))
                        .append(";\n");
            }
        }

        ClassUtil.forName(BinlogUtil.DRIVER_NAME, getClass().getClassLoader());
        Properties properties = new Properties();
        properties.put("user", binlogConfig.getUsername());
        properties.put("password", binlogConfig.getPassword());
        properties.put("socketTimeout", String.valueOf(binlogConfig.getQueryTimeOut()));
        properties.put("connectTimeout", String.valueOf(binlogConfig.getConnectTimeOut()));

        try (Connection conn =
                RetryUtil.executeWithRetry(
                        () -> DriverManager.getConnection(binlogConfig.getJdbcUrl(), properties),
                        BinlogUtil.RETRY_TIMES,
                        BinlogUtil.SLEEP_TIME,
                        false)) {

            // 校验用户权限
            if (!BinlogUtil.checkUserPrivilege(conn)) {
                sb.append(
                                "\nyou need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation; you can execute sql ->")
                        .append("GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '")
                        .append(binlogConfig.getUsername())
                        .append("'@'%' IDENTIFIED BY 'password';\n\n");
            }

            // 校验数据库是否开启binlog
            if (!BinlogUtil.checkEnabledBinlog(conn)) {
                sb.append(
                                "binlog has not enabled, please click my.cnf Add the following to the file: \n")
                        .append("server_id=109\n")
                        .append("log_bin = /var/lib/mysql/mysql-bin\n")
                        .append("binlog_format = ROW\n")
                        .append("expire_logs_days = 30\n\n");
            }

            // 校验数据库binlog_format是否设置为row
            if (!BinlogUtil.checkBinlogFormat(conn)) {
                sb.append(" binlog_format must be set ROW ;\n");
            }

            // 校验用户表是否有select权限
            String database = BinlogUtil.getDataBaseByUrl(binlogConfig.jdbcUrl);
            List<String> failedTable =
                    BinlogUtil.checkTablesPrivilege(
                            conn, database, binlogConfig.filter, binlogConfig.table);
            if (CollectionUtils.isNotEmpty(failedTable)) {
                sb.append("user has not select privilege on ")
                        .append(GsonUtil.GSON.toJson(failedTable));
            }

            if (sb.length() > 0) {
                throw new IllegalArgumentException(sb.toString());
            }

        } catch (SQLException e) {
            StringBuilder detailsInfo = new StringBuilder(sb.length() + 128);
            if (sb.length() > 0) {
                detailsInfo.append(" binlog config not right，details is  ").append(sb);
            }
            detailsInfo
                    .append(" \n error to check binlog config, e = ")
                    .append(ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(detailsInfo.toString(), e);
        }
    }
}
