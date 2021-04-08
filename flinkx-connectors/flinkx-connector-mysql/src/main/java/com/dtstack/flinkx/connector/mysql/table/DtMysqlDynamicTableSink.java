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

import com.dtstack.flinkx.connector.jdbc.table.DtJdbcDynamicTableSink;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/03/17
 **/
public class DtMysqlDynamicTableSink extends DtJdbcDynamicTableSink {

    public DtMysqlDynamicTableSink(JdbcOptions jdbcOptions, JdbcExecutionOptions executionOptions, JdbcDmlOptions dmlOptions, TableSchema tableSchema) {
        super(jdbcOptions, executionOptions, dmlOptions, tableSchema);
    }

    @Override
    public DynamicTableSink copy() {
        return new DtMysqlDynamicTableSink(
                this.jdbcOptions,
                this.executionOptions,
                this.dmlOptions,
                this.tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "DTStack mysql table sink";
    }
}
