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

package com.dtstack.flinkx.connector.mysql.source;

import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.connector.jdbc.lookup.JdbcAllTableFunction;
import com.dtstack.flinkx.connector.jdbc.lookup.JdbcLruTableFunction;
import com.dtstack.flinkx.connector.jdbc.source.JdbcDynamicTableSource;
import com.dtstack.flinkx.connector.mysql.lookup.MysqlAllTableFunction;
import com.dtstack.flinkx.connector.mysql.lookup.MysqlLruTableFunction;
import com.dtstack.flinkx.lookup.options.LookupOptions;

/**
 * @author chuixue
 * @create 2021-04-10 14:09
 * @description
 **/
public class MysqlDynamicTableSource extends JdbcDynamicTableSource {

    public MysqlDynamicTableSource(
            JdbcOptions options,
            JdbcReadOptions readOptions,
            LookupOptions lookupOptions,
            TableSchema physicalSchema) {
        super(options, readOptions, lookupOptions, physicalSchema);
    }

    @Override
    protected JdbcAllTableFunction createAllTableFunction(
            JdbcOptions options,
            LookupOptions lookupOptions,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        return new MysqlAllTableFunction(
                options,
                lookupOptions,
                physicalSchema.getFieldNames(),
                keyNames,
                rowType
        );
    }

    @Override
    protected JdbcLruTableFunction createLruTableFunction(
            JdbcOptions options,
            LookupOptions lookupOptions,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        return new MysqlLruTableFunction(
                options,
                lookupOptions,
                physicalSchema.getFieldNames(),
                keyNames,
                rowType
        );
    }

    @Override
    public DynamicTableSource copy() {
        return new MysqlDynamicTableSource(options, readOptions, lookupOptions, physicalSchema);
    }
}
