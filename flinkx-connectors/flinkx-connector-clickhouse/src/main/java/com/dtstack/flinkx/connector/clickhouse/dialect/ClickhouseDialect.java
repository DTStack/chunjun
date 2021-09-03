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

package com.dtstack.flinkx.connector.clickhouse.dialect;

import com.dtstack.flinkx.connector.clickhouse.converter.ClickhouseRawTypeConverter;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputSplit;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;

import java.util.Optional;

/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/05/08
 */
public class ClickhouseDialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return "ClickHouse";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:clickhouse:");
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return ClickhouseRawTypeConverter::apply;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("ru.yandex.clickhouse.ClickHouseDriver");
    }

    @Override
    public String getUpdateStatement(
            String schema, String tableName, String[] fieldNames, String[] conditionFields) {
        throw new FlinkxRuntimeException("Clickhouse does not support update sql");
    }

    @Override
    public Optional<String> getReplaceStatement(
            String schema, String tableName, String[] fieldNames) {
        throw new FlinkxRuntimeException("Clickhouse does not support replace sql");
    }

    @Override
    public String getDeleteStatement(String schema, String tableName, String[] conditionFields) {
        throw new FlinkxRuntimeException("Clickhouse does not support delete sql");
    }

    @Override
    public String getSplitModFilter(JdbcInputSplit split, String splitPkName) {
        return String.format(
                " modulo(%s,%s) = %s",
                quoteIdentifier(splitPkName), split.getTotalNumberOfSplits(), split.getMod());
    }
}
