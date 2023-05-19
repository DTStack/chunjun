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

package com.dtstack.chunjun.connector.clickhouse.dialect;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.clickhouse.converter.ClickhouseRawTypeConverter;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputSplit;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class ClickhouseDialect implements JdbcDialect {

    private static final long serialVersionUID = 4485804663355512770L;

    @Override
    public String dialectName() {
        return "ClickHouse";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:clickhouse:");
    }

    @Override
    public RawTypeMapper getRawTypeConverter() {
        return ClickhouseRawTypeConverter::apply;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("ru.yandex.clickhouse.ClickHouseDriver");
    }

    @Override
    public String getUpdateStatement(
            String schema, String tableName, String[] fieldNames, String[] conditionFields) {
        throw new ChunJunRuntimeException("Clickhouse does not support update sql");
    }

    @Override
    public Optional<String> getReplaceStatement(
            String schema, String tableName, String[] fieldNames) {
        throw new ChunJunRuntimeException("Clickhouse does not support replace sql");
    }

    @Override
    public String getKeyedDeleteStatement(
            String schema, String tableName, List<String> conditionFieldList) {
        throw new ChunJunRuntimeException("Clickhouse does not support delete sql");
    }

    @Override
    public String getDeleteStatement(
            String schema,
            String tableName,
            String[] conditionFields,
            String[] nullConditionFields) {
        throw new ChunJunRuntimeException("Clickhouse does not support delete sql");
    }

    @Override
    public String getSplitModFilter(JdbcInputSplit split, String splitPkName) {
        return String.format(
                " modulo(%s,%s) = %s",
                quoteIdentifier(splitPkName), split.getTotalNumberOfSplits(), split.getMod());
    }

    @Override
    public Function<Tuple3<String, Integer, Integer>, TypeConfig> typeBuilder() {
        return typeInfoTuple3 -> {
            String type = typeInfoTuple3.f0;
            TypeConfig typeConfig = TypeConfig.fromString(type);
            if (typeConfig.getType().contains("DATETIME64")) {
                return typeConfig;
            }
            if (typeInfoTuple3.f1 != null) {
                typeConfig.setPrecision(typeInfoTuple3.f1);
            }
            if (typeInfoTuple3.f2 != null) {
                typeConfig.setScale(typeInfoTuple3.f2);
            }
            return typeConfig;
        };
    }
}
