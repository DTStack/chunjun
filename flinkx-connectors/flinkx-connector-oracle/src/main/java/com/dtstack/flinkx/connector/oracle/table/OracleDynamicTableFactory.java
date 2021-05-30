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

package com.dtstack.flinkx.connector.oracle.table;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.source.JdbcDynamicTableSource;
import com.dtstack.flinkx.connector.jdbc.table.JdbcDynamicTableFactory;
import com.dtstack.flinkx.connector.oracle.OracleDialect;

import com.dtstack.flinkx.connector.oracle.source.OracleDynamicTableSource;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleDynamicTableFactory extends JdbcDynamicTableFactory {

    /** 通过该值查找具体插件 */
    private static final String IDENTIFIER = "oracle-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected JdbcDialect getDialect() {
        return new OracleDialect();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();
        validateConfigOptions(config);

        // 3.封装参数
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        JdbcDialect jdbcDialect = getDialect();

        return new OracleDynamicTableSource(
                getSourceConnectionConf(helper.getOptions()),
                getJdbcLookupConf(
                        helper.getOptions(), context.getObjectIdentifier().getObjectName()),
                physicalSchema,
                jdbcDialect);
    }

}
