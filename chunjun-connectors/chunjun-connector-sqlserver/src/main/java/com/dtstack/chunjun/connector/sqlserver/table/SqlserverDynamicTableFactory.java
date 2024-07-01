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

package com.dtstack.chunjun.connector.sqlserver.table;

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.table.JdbcDynamicTableFactory;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.sqlserver.dialect.SqlserverDialect;
import com.dtstack.chunjun.connector.sqlserver.sink.SqlserverOutputFormat;
import com.dtstack.chunjun.connector.sqlserver.source.SqlserverInputFormat;

import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class SqlserverDynamicTableFactory extends JdbcDynamicTableFactory {

    private static final String IDENTIFIER = "sqlserver-x";

    private JdbcConfig jdbcConfig;

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected JdbcDialect getDialect() {
        if (jdbcConfig != null) {
            return new SqlserverDialect(
                    jdbcConfig.isWithNoLock(),
                    jdbcConfig.getJdbcUrl().startsWith("jdbc:jtds:sqlserver"));
        }
        return new SqlserverDialect();
    }

    @Override
    protected JdbcInputFormatBuilder getInputFormatBuilder() {
        return new JdbcInputFormatBuilder(new SqlserverInputFormat());
    }

    @Override
    protected JdbcOutputFormatBuilder getOutputFormatBuilder() {
        return new JdbcOutputFormatBuilder(new SqlserverOutputFormat());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        this.jdbcConfig = getSourceConnectionConfig(helper.getOptions());
        Map<String, String> prop = context.getCatalogTable().getOptions();
        prop.put("druid.validation-query", "SELECT 1");
        return super.createDynamicTableSource(context);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        this.jdbcConfig =
                getSinkConnectionConfig(
                        helper.getOptions(), context.getCatalogTable().getResolvedSchema());
        return super.createDynamicTableSink(context);
    }

    /** table字段有可能是[schema].[table]格式 需要转换为对应的schema 和 table 字段* */
    @Override
    protected void resetTableInfo(JdbcConfig jdbcConfig) {
        if (jdbcConfig.getTable().startsWith("[")
                && jdbcConfig.getTable().endsWith("]")
                && StringUtils.isBlank(jdbcConfig.getSchema())) {
            JdbcUtil.resetSchemaAndTable(jdbcConfig, "\\[", "\\]");
        }
    }
}
