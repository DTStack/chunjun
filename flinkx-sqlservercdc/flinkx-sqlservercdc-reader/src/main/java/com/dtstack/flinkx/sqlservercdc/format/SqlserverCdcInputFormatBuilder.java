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
package com.dtstack.flinkx.sqlservercdc.format;

import com.dtstack.flinkx.config.SpeedConfig;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.sqlservercdc.SqlServerCdcUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Set;

import static com.dtstack.flinkx.sqlservercdc.SqlServerCdcUtil.DRIVER;

/**
 * Date: 2019/12/03
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class SqlserverCdcInputFormatBuilder extends BaseRichInputFormatBuilder {

    protected SqlserverCdcInputFormat format;

    public SqlserverCdcInputFormatBuilder() {
        super.format = this.format = new SqlserverCdcInputFormat();
    }

    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setUrl(String url) {
        format.url = url;
    }

    public void setDatabaseName(String databaseName) {
        format.databaseName = databaseName;
    }

    public void setPavingData(boolean pavingData) {
        format.pavingData = pavingData;
    }

    public void setTable(List<String> table) {
        format.tableList = table;
    }

    public void setCat(String cat) {
        format.cat = cat;
    }

    public void setPollInterval(long pollInterval) {
        format.pollInterval = pollInterval;
    }

    public void setLsn(String lsn) {
        format.lsn = lsn;
    }


    @Override
    protected void checkFormat() {
        StringBuilder sb = new StringBuilder(256);

        if (StringUtils.isBlank(format.username)) {
            sb.append("No username supplied;\n");
        }
        if (StringUtils.isBlank(format.password)) {
            sb.append("No password supplied;\n");
        }
        if (StringUtils.isBlank(format.url)) {
            sb.append("No url supplied;\n");
        }
        if (StringUtils.isBlank(format.databaseName)) {
            sb.append("No databaseName supplied;\n");
        }
        if (CollectionUtils.isEmpty(format.tableList)) {
            sb.append("No tableList supplied;\n");
        }
        if (StringUtils.isBlank(format.cat)) {
            sb.append("No cat supplied;\n");
        }
        SpeedConfig speed = format.getDataTransferConfig().getJob().getSetting().getSpeed();
        if(speed.getReaderChannel() > 1){
            sb.append("sqlServerCdc can not support readerChannel bigger than 1, current readerChannel is [")
                    .append(speed.getReaderChannel())
                    .append("];\n");
        }else if(speed.getChannel() > 1){
            sb.append("sqlServerCdc can not support channel bigger than 1, current channel is [")
                    .append(speed.getChannel())
                    .append("];\n");
        }

        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }

        try {
            ClassUtil.forName(DRIVER, getClass().getClassLoader());
            try (Connection conn = SqlServerCdcUtil.getConnection(format.url, format.username, format.password)) {

                //效验是否开启agent
                if (!SqlServerCdcUtil.checkAgentHasStart(conn)) {
                    sb.append("\n\nsqlServer agentServer not running,please enable agentServer;");
                }

                //校验数据库是否开启cdc
                SqlServerCdcUtil.changeDatabase(conn, format.databaseName);
                if (!SqlServerCdcUtil.checkEnabledCdcDatabase(conn, format.databaseName)) {
                    sb.append(format.databaseName).append(" is not enable sqlServer CDC;\n")
                            .append("please execute sql for enable databaseCDC：\nUSE ").append(format.databaseName).append("\nGO\nEXEC sys.sp_cdc_enable_db\nGO\n\n ");
                }
                //效验表是否开启cdc
                Set<String> unEnabledCdcTables = SqlServerCdcUtil.checkUnEnabledCdcTables(conn, format.tableList);
                if (CollectionUtils.isNotEmpty(unEnabledCdcTables)) {
                    String tables = unEnabledCdcTables.toString();
                    sb.append(GsonUtil.GSON.toJson(tables)).append("  is not enable sqlServer CDC;\n")
                            .append("please execute sql for enable tableCDC: ");
                    String tableEnableCdcTemplate = "\n\n EXEC sys.sp_cdc_enable_table \n@source_schema = '%s',\n@source_name = '%s',\n@role_name = NULL,\n@supports_net_changes = 0;";

                    for (String table : unEnabledCdcTables) {
                        List<String> strings = StringUtil.splitIgnoreQuota(table, ConstantValue.POINT_SYMBOL.charAt(0));
                        if (strings.size() == 2) {
                            sb.append(String.format(tableEnableCdcTemplate, strings.get(0), strings.get(1)));
                        } else if (strings.size() == 1) {
                            sb.append(String.format(tableEnableCdcTemplate, "yourSchema", strings.get(0)));
                        }
                    }
                }


                if (sb.length() > 0) {
                    throw new IllegalArgumentException(sb.toString());
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("error to check sqlServerCDC config, e = " + ExceptionUtil.getErrorMessage(e), e);
        }
    }
}
