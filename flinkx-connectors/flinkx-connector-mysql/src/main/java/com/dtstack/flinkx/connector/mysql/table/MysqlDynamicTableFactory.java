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

package com.dtstack.flinkx.connector.mysql.table;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.FactoryUtil;

import com.dtstack.flinkx.connector.jdbc.source.JdbcDynamicTableSource;
import com.dtstack.flinkx.connector.jdbc.table.JdbcDynamicTableFactory;
import com.dtstack.flinkx.connector.mysql.MySQLDialect;
import com.dtstack.flinkx.connector.mysql.source.MysqlDynamicTableSource;

import java.util.Optional;

import static com.dtstack.flinkx.connector.mysql.constants.MysqlConstants.MYSQL_DRIVER;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/03/17
 **/
public class MysqlDynamicTableFactory extends JdbcDynamicTableFactory {

    /** 通过该值查找具体插件 */
    private static final String IDENTIFIER = "dtmysql";

    @Override
    protected Optional<String> getDriver() {
        return Optional.of(MYSQL_DRIVER);
    }

    @Override
    protected JdbcDynamicTableSource createTableSource(
            FactoryUtil.TableFactoryHelper helper,
            Context context,
            TableSchema physicalSchema) {
        return new MysqlDynamicTableSource(
                getJdbcOptions(helper.getOptions()),
                getJdbcReadOptions(helper.getOptions()),
                getJdbcLookupOptions(
                        helper.getOptions(),
                        context.getObjectIdentifier().getObjectName()),
                physicalSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    protected JdbcDialect getDialect() {
        return new MySQLDialect();
    }
}
