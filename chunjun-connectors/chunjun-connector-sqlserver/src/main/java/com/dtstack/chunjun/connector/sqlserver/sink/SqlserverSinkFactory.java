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

package com.dtstack.chunjun.connector.sqlserver.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcSinkFactory;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.sqlserver.converter.SqlserverJtdsRawTypeMapper;
import com.dtstack.chunjun.connector.sqlserver.converter.SqlserverMicroSoftRawTypeMapper;
import com.dtstack.chunjun.connector.sqlserver.dialect.SqlserverDialect;
import com.dtstack.chunjun.converter.RawTypeMapper;

import org.apache.commons.lang3.StringUtils;

public class SqlserverSinkFactory extends JdbcSinkFactory {

    private final boolean useJtdsDriver;

    public SqlserverSinkFactory(SyncConfig syncConfig) {
        super(syncConfig, null);
        useJtdsDriver = jdbcConfig.getJdbcUrl().startsWith("jdbc:jtds:sqlserver");
        jdbcDialect = new SqlserverDialect(jdbcConfig.isWithNoLock(), useJtdsDriver);
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        return new JdbcOutputFormatBuilder(new SqlserverOutputFormat());
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        if (useJtdsDriver) {
            return SqlserverJtdsRawTypeMapper::apply;
        }
        return SqlserverMicroSoftRawTypeMapper::apply;
    }

    /** table字段有可能是[schema].[table]格式 需要转换为对应的schema 和 table 字段* */
    @Override
    protected void resetTableInfo() {
        if (jdbcConfig.getTable().startsWith("[")
                && jdbcConfig.getTable().endsWith("]")
                && StringUtils.isBlank(jdbcConfig.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConfig, "\\[", "\\]");
        } else {
            super.resetTableInfo();
        }
    }
}
