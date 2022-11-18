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

import com.dtstack.chunjun.config.SyncConf;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcSinkFactory;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.sqlserver.converter.SqlserverJtdsRawTypeConverter;
import com.dtstack.chunjun.connector.sqlserver.converter.SqlserverMicroSoftRawTypeConverter;
import com.dtstack.chunjun.connector.sqlserver.dialect.SqlserverDialect;
import com.dtstack.chunjun.converter.RawTypeConverter;

import org.apache.commons.lang3.StringUtils;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/5/19 20:11
 */
public class SqlserverSinkFactory extends JdbcSinkFactory {

    private boolean useJtdsDriver;

    public SqlserverSinkFactory(SyncConf syncConf) {
        super(syncConf, null);
        useJtdsDriver = jdbcConf.getJdbcUrl().startsWith("jdbc:jtds:sqlserver");
        jdbcDialect = new SqlserverDialect(jdbcConf.isWithNoLock(), useJtdsDriver);
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        return new JdbcOutputFormatBuilder(new SqlserverOutputFormat());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        if (useJtdsDriver) {
            return SqlserverJtdsRawTypeConverter::apply;
        }
        return SqlserverMicroSoftRawTypeConverter::apply;
    }

    /** table字段有可能是[schema].[table]格式 需要转换为对应的schema 和 table 字段* */
    @Override
    protected void resetTableInfo() {
        if (jdbcConf.getTable().startsWith("[")
                && jdbcConf.getTable().endsWith("]")
                && StringUtils.isBlank(jdbcConf.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConf, "\\[", "\\]");
        } else {
            super.resetTableInfo();
        }
    }
}
