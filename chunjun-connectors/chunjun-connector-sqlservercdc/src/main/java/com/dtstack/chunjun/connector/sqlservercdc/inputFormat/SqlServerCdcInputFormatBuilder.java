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
package com.dtstack.chunjun.connector.sqlservercdc.inputFormat;

import com.dtstack.chunjun.connector.sqlservercdc.conf.SqlServerCdcConf;
import com.dtstack.chunjun.connector.sqlservercdc.entity.Lsn;
import com.dtstack.chunjun.connector.sqlservercdc.entity.SqlServerCdcEnum;
import com.dtstack.chunjun.connector.sqlservercdc.util.SqlServerCdcUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractCDCRowConverter;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.RetryUtil;
import com.dtstack.chunjun.util.StringUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static com.dtstack.chunjun.connector.sqlservercdc.util.SqlServerCdcUtil.DRIVER;

/**
 * Date: 2019/12/03 Company: www.dtstack.com
 *
 * @author tudou
 */
public class SqlServerCdcInputFormatBuilder
        extends BaseRichInputFormatBuilder<SqlServerCdcInputFormat> {

    protected String tableFormat = "%s.%s";

    protected SqlServerCdcInputFormat format;

    public SqlServerCdcInputFormatBuilder() {
        super(new SqlServerCdcInputFormat());
    }

    public void setSqlServerCdcConf(SqlServerCdcConf sqlServerCdcConf) {
        super.setConfig(sqlServerCdcConf);
        this.format.setSqlServerCdcConf(sqlServerCdcConf);
    }

    public void setRowConverter(AbstractCDCRowConverter rowConverter) {
        this.format.setRowConverter(rowConverter);
    }

    @Override
    protected void checkFormat() {
        StringBuilder sb = new StringBuilder(256);

        if (StringUtils.isBlank(format.sqlserverCdcConf.getUsername())) {
            sb.append("No username supplied;\n");
        }
        if (StringUtils.isBlank(format.sqlserverCdcConf.getPassword())) {
            sb.append("No password supplied;\n");
        }
        if (StringUtils.isBlank(format.sqlserverCdcConf.getUrl())) {
            sb.append("No url supplied;\n");
        }
        if (StringUtils.isBlank(format.sqlserverCdcConf.getDatabaseName())) {
            sb.append("No databaseName supplied;\n");
        }
        if (StringUtils.isBlank(format.sqlserverCdcConf.getCat())) {
            sb.append("No cat supplied;\n");
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }

        // 校验cat
        HashSet<String> set =
                Sets.newHashSet(
                        SqlServerCdcEnum.DELETE.name,
                        SqlServerCdcEnum.UPDATE.name,
                        SqlServerCdcEnum.INSERT.name);
        ArrayList<String> cats =
                Lists.newArrayList(
                        format.sqlserverCdcConf.getCat().split(ConstantValue.COMMA_SYMBOL));
        cats.removeIf(s -> set.contains(s.toLowerCase(Locale.ENGLISH)));
        if (CollectionUtils.isNotEmpty(cats)) {
            sb.append("sqlServer cat not support-> ")
                    .append(GsonUtil.GSON.toJson(cats))
                    .append(",just support->")
                    .append(GsonUtil.GSON.toJson(set))
                    .append(";\n");
        }

        ClassUtil.forName(DRIVER, getClass().getClassLoader());
        try (Connection conn =
                RetryUtil.executeWithRetry(
                        () ->
                                SqlServerCdcUtil.getConnection(
                                        format.sqlserverCdcConf.getUrl(),
                                        format.sqlserverCdcConf.getUsername(),
                                        format.sqlserverCdcConf.getPassword()),
                        SqlServerCdcUtil.RETRY_TIMES,
                        SqlServerCdcUtil.SLEEP_TIME,
                        false)) {

            // check database cdc is enable
            SqlServerCdcUtil.changeDatabase(conn, format.sqlserverCdcConf.getDatabaseName());
            if (!SqlServerCdcUtil.checkEnabledCdcDatabase(
                    conn, format.sqlserverCdcConf.getDatabaseName())) {
                sb.append(format.sqlserverCdcConf.getDatabaseName())
                        .append(" is not enable sqlServer CDC;\n")
                        .append("please execute sql for enable databaseCDC：\nUSE ")
                        .append(format.sqlserverCdcConf.getDatabaseName())
                        .append("\nGO\nEXEC sys.sp_cdc_enable_db\nGO\n\n ");
            }

            if (sb.length() > 0) {
                throw new IllegalArgumentException(sb.toString());
            }
            List<String> formatTables = new ArrayList<>();
            format.sqlserverCdcConf
                    .getTableList()
                    .forEach(
                            e -> {
                                List<String> strings =
                                        StringUtil.splitIgnoreQuota(
                                                e, ConstantValue.POINT_SYMBOL.charAt(0));
                                if (strings.size() == 2) {
                                    formatTables.add(
                                            String.format(
                                                    tableFormat, strings.get(0), strings.get(1)));
                                } else {
                                    formatTables.add(strings.get(0));
                                }
                            });
            format.sqlserverCdcConf.setTableList(formatTables);
            // check table cdc is enable
            Set<String> unEnabledCdcTables =
                    SqlServerCdcUtil.checkUnEnabledCdcTables(
                            conn, format.sqlserverCdcConf.getTableList());
            if (CollectionUtils.isNotEmpty(unEnabledCdcTables)) {
                String tables = unEnabledCdcTables.toString();
                sb.append(GsonUtil.GSON.toJson(tables))
                        .append("  is not enable sqlServer CDC;\n")
                        .append("please execute sql for enable tableCDC: ");
                String tableEnableCdcTemplate =
                        "\n\n EXEC sys.sp_cdc_enable_table \n@source_schema = '%s',\n@source_name = '%s',\n@role_name = NULL,\n@supports_net_changes = 0;";

                for (String table : unEnabledCdcTables) {
                    List<String> strings =
                            StringUtil.splitIgnoreQuota(
                                    table, ConstantValue.POINT_SYMBOL.charAt(0));
                    if (strings.size() == 2) {
                        sb.append(
                                String.format(
                                        tableEnableCdcTemplate, strings.get(0), strings.get(1)));
                    } else if (strings.size() == 1) {
                        sb.append(
                                String.format(
                                        tableEnableCdcTemplate, "yourSchema", strings.get(0)));
                    }
                }
            }

            // check lsn if over max lsn
            Lsn currentMaxLsn = SqlServerCdcUtil.getMaxLsn(conn);
            if (StringUtils.isNotBlank(format.sqlserverCdcConf.getLsn())) {
                if (currentMaxLsn.compareTo(Lsn.valueOf(format.sqlserverCdcConf.getLsn())) < 0) {
                    sb.append("lsn: '")
                            .append(format.sqlserverCdcConf.getLsn())
                            .append("' does not exist;\n");
                }
            }

            if (sb.length() > 0) {
                throw new IllegalArgumentException(sb.toString());
            }

        } catch (SQLException e) {
            StringBuilder detailsInfo = new StringBuilder(sb.length() + 128);

            if (sb.length() > 0) {
                detailsInfo
                        .append("sqlserverCDC config not right，details is ")
                        .append(sb.toString());
            }

            detailsInfo
                    .append(" \n error to check sqlServerCDC config, e = ")
                    .append(ExceptionUtil.getErrorMessage(e));

            throw new RuntimeException(detailsInfo.toString(), e);
        }
    }
}
