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

package com.dtstack.flinkx.connector.oracle.source;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.lookup.JdbcLruTableFunction;
import com.dtstack.flinkx.connector.jdbc.source.JdbcDynamicTableSource;
import com.dtstack.flinkx.connector.oracle.lookup.OracleAllTableFunction;
import com.dtstack.flinkx.enums.CacheType;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.table.connector.source.ParallelAsyncTableFunctionProvider;
import com.dtstack.flinkx.table.connector.source.ParallelTableFunctionProvider;


/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleDynamicTableSource
        extends JdbcDynamicTableSource {

    public OracleDynamicTableSource(
            JdbcConf jdbcConf,
            LookupConf lookupConf,
            TableSchema physicalSchema,
            JdbcDialect jdbcDialect) {
        super(jdbcConf,lookupConf,physicalSchema,jdbcDialect);
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
        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        if (lookupConf.getCache().equalsIgnoreCase(CacheType.LRU.toString())) {
            return ParallelAsyncTableFunctionProvider.of(
                    new JdbcLruTableFunction(
                            jdbcConf,
                            jdbcDialect,
                            lookupConf,
                            physicalSchema.getFieldNames(),
                            keyNames,
                            rowType),
                    lookupConf.getParallelism());
        }
        return ParallelTableFunctionProvider.of(
                new OracleAllTableFunction(
                        jdbcConf,
                        jdbcDialect,
                        lookupConf,
                        physicalSchema.getFieldNames(),
                        keyNames,
                        rowType),
                lookupConf.getParallelism());
    }


}
