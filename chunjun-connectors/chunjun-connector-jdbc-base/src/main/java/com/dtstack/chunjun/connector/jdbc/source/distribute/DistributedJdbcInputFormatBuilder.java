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
package com.dtstack.chunjun.connector.jdbc.source.distribute;

import com.dtstack.chunjun.connector.jdbc.config.ConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.config.DataSourceConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class DistributedJdbcInputFormatBuilder extends JdbcInputFormatBuilder {

    public DistributedJdbcInputFormatBuilder(DistributedJdbcInputFormat format) {
        super(format);
    }

    public void setSourceList(List<DataSourceConfig> sourceList) {
        DistributedJdbcInputFormat format = (DistributedJdbcInputFormat) this.format;
        format.setSourceList(sourceList);
    }

    @Override
    protected void checkFormat() {
        JdbcConfig conf = format.getJdbcConfig();
        StringBuilder sb = new StringBuilder(256);
        boolean hasGlobalAccountInfo =
                !StringUtils.isBlank(conf.getUsername())
                        && !StringUtils.isBlank(conf.getPassword());

        if (conf.getConnection() == null || conf.getConnection().size() == 0) {
            sb.append("One or more data sources must be specified;\n");
        }

        if (StringUtils.isNotBlank(conf.getRestoreColumn())) {
            sb.append("JDBC distribute plugin not support restore from failed state;\n");
        }

        for (ConnectionConfig connectionConfig : conf.getConnection()) {
            boolean hasNoAccountInfoInConnectionConf =
                    StringUtils.isBlank(connectionConfig.getUsername())
                            || StringUtils.isBlank(connectionConfig.getPassword());
            if (hasNoAccountInfoInConnectionConf && !hasGlobalAccountInfo) {
                sb.append(
                        "Must specify a global account or specify an account for each data source;\n");
            }

            if (connectionConfig.getTable() == null || connectionConfig.getTable().size() == 0) {
                sb.append("Table name cannot be empty;\n");
            }

            if (connectionConfig.obtainJdbcUrl() == null
                    || connectionConfig.obtainJdbcUrl().length() == 0) {
                sb.append("JDBC url cannot be empty;\n");
            }
        }

        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
