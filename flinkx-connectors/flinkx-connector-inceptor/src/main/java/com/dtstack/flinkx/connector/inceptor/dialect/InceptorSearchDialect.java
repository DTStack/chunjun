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

package com.dtstack.flinkx.connector.inceptor.dialect;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.connector.inceptor.converter.InceptorSearchColumnConverter;
import com.dtstack.flinkx.connector.inceptor.converter.InceptorSearchRawTypeConverter;
import com.dtstack.flinkx.connector.inceptor.converter.InceptorSearchRowConverter;
import com.dtstack.flinkx.connector.inceptor.sink.InceptorSearchOutputFormatBuilder;
import com.dtstack.flinkx.connector.inceptor.source.InceptorSearchInputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;

import java.sql.ResultSet;

/** @author liuliu 2022/2/23 */
public class InceptorSearchDialect extends InceptorDialect {
    @Override
    public RawTypeConverter getRawTypeConverter() {
        return InceptorSearchRawTypeConverter::apply;
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getRowConverter(RowType rowType) {
        return new InceptorSearchRowConverter(rowType);
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getColumnConverter(RowType rowType, FlinkxCommonConf commonConf) {
        return new InceptorSearchColumnConverter(rowType, commonConf);
    }

    @Override
    public JdbcInputFormatBuilder getInputFormatBuilder() {
        return new InceptorSearchInputFormatBuilder();
    }

    @Override
    public JdbcOutputFormatBuilder getOutputFormatBuilder() {
        return new InceptorSearchOutputFormatBuilder();
    }
}
