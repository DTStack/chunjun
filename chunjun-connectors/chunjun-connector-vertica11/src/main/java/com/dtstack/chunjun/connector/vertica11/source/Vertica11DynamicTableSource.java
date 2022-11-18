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

import com.dtstack.chunjun.connector.jdbc.config.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.lookup.JdbcAllTableFunction;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.vertica11.lookup.Vertica11LruTableFunction;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.conf.LookupConf;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncTableFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelTableFunctionProvider;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

/** @author menghan on 2022/7/24. */
public class Vertica11DynamicTableSource implements LookupTableSource, SupportsProjectionPushDown {

    protected final JdbcConf jdbcConf;
    protected final LookupConf lookupConf;
    protected final String dialectName;
    protected final JdbcDialect jdbcDialect;
    protected final JdbcInputFormatBuilder builder;
    protected TableSchema physicalSchema;

    public Vertica11DynamicTableSource(
            JdbcConf jdbcConf,
            LookupConf lookupConf,
            TableSchema physicalSchema,
            JdbcDialect jdbcDialect,
            JdbcInputFormatBuilder builder) {
        this.jdbcConf = jdbcConf;
        this.lookupConf = lookupConf;
        this.physicalSchema = physicalSchema;
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
            keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
        }
        // Get the type converter through this parameter to convert the fields in the database to
        // the corresponding types
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        if (lookupConf.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelTableFunctionProvider.of(
                    new JdbcAllTableFunction(
                            jdbcConf,
                            jdbcDialect,
                            lookupConf,
                            physicalSchema.getFieldNames(),
                            keyNames,
                            rowType),
                    lookupConf.getParallelism());
        }
        return ParallelAsyncTableFunctionProvider.of(
                new Vertica11LruTableFunction(
                        jdbcConf,
                        jdbcDialect,
                        lookupConf,
                        physicalSchema.getFieldNames(),
                        keyNames,
                        rowType),
                lookupConf.getParallelism());
    }

    @Override
    public boolean supportsNestedProjection() {
        // JDBC doesn't support nested projection
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
    }

    @Override
    public DynamicTableSource copy() {
        return new Vertica11DynamicTableSource(
                jdbcConf, lookupConf, physicalSchema, jdbcDialect, builder);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }
}
