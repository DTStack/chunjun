/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.vertica11.source;

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.lookup.JdbcAllTableFunction;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.vertica11.lookup.Vertica11LruTableFunction;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelLookupFunctionProvider;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

public class Vertica11DynamicTableSource implements LookupTableSource, SupportsProjectionPushDown {

    protected final JdbcConfig jdbcConfig;
    protected final LookupConfig lookupConfig;
    protected final String dialectName;
    protected final JdbcDialect jdbcDialect;
    protected final JdbcInputFormatBuilder builder;
    protected final ResolvedSchema resolvedSchema;

    public Vertica11DynamicTableSource(
            JdbcConfig jdbcConfig,
            LookupConfig lookupConfig,
            ResolvedSchema resolvedSchema,
            JdbcDialect jdbcDialect,
            JdbcInputFormatBuilder builder) {
        this.jdbcConfig = jdbcConfig;
        this.lookupConfig = lookupConfig;
        this.resolvedSchema = resolvedSchema;
        this.jdbcDialect = jdbcDialect;
        this.dialectName = jdbcDialect.dialectName();
        this.builder = builder;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = resolvedSchema.getColumnNames().get(innerKeyArr[0]);
        }
        // Get the type converter through this parameter to convert the fields in the database to
        // the corresponding types
        final RowType rowType =
                InternalTypeInfo.of(resolvedSchema.toPhysicalRowDataType().getLogicalType())
                        .toRowType();

        if (lookupConfig.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelLookupFunctionProvider.of(
                    new JdbcAllTableFunction(
                            jdbcConfig,
                            jdbcDialect,
                            lookupConfig,
                            resolvedSchema.getColumnNames().toArray(new String[0]),
                            keyNames,
                            rowType),
                    lookupConfig.getParallelism());
        }
        return ParallelAsyncLookupFunctionProvider.of(
                new Vertica11LruTableFunction(
                        jdbcConfig,
                        jdbcDialect,
                        lookupConfig,
                        resolvedSchema.getColumnNames().toArray(new String[0]),
                        keyNames,
                        rowType),
                lookupConfig.getParallelism());
    }

    @Override
    public boolean supportsNestedProjection() {
        // JDBC doesn't support nested projection
        return false;
    }

    @Override
    public DynamicTableSource copy() {
        return new Vertica11DynamicTableSource(
                jdbcConfig, lookupConfig, resolvedSchema, jdbcDialect, builder);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }
}
