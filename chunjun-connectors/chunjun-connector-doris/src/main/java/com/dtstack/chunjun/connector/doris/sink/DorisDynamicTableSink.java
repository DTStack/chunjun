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

package com.dtstack.chunjun.connector.doris.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.doris.converter.DorisHttpSqlConverter;
import com.dtstack.chunjun.connector.doris.converter.DorisJdbcSqlConverter;
import com.dtstack.chunjun.connector.doris.options.DorisConfig;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcDynamicTableSink;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.chunjun.connector.mysql.dialect.MysqlDialect;
import com.dtstack.chunjun.enums.EWriteMode;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CollectionUtil;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class DorisDynamicTableSink extends JdbcDynamicTableSink {

    private final ResolvedSchema physicalSchema;

    private final DorisConfig dorisConfig;

    public DorisDynamicTableSink(ResolvedSchema physicalSchema, DorisConfig dorisConfig) {
        super(
                dorisConfig.setToJdbcConf(),
                new MysqlDialect(),
                physicalSchema,
                new JdbcOutputFormatBuilder(new JdbcOutputFormat()));
        this.physicalSchema = physicalSchema;
        this.dorisConfig = dorisConfig;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        String url = dorisConfig.getUrl();

        RowType rowType =
                InternalTypeInfo.of(tableSchema.toPhysicalRowDataType().getLogicalType())
                        .toRowType();
        BaseRichOutputFormatBuilder<?> builder =
                StringUtils.isBlank(url)
                        ? httpBuilder(rowType, dorisConfig)
                        : jdbcBuilder(rowType, dorisConfig);

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), dorisConfig.getParallelism());
    }

    private DorisHttpOutputFormatBuilder httpBuilder(RowType rowType, DorisConfig dorisConfig) {
        DorisHttpOutputFormatBuilder builder = new DorisHttpOutputFormatBuilder();
        builder.setColumns(tableSchema.getColumnNames());
        builder.setConfig(dorisConfig);
        builder.setDorisOptions(dorisConfig);
        builder.setRowConverter(new DorisHttpSqlConverter(rowType));
        return builder;
    }

    private JdbcOutputFormatBuilder jdbcBuilder(RowType rowType, DorisConfig dorisConfig) {
        JdbcOutputFormatBuilder builder = new JdbcOutputFormatBuilder(new JdbcOutputFormat());

        List<Column> columns = tableSchema.getColumns();
        List<String> columnNameList = new ArrayList<>(columns.size());
        List<TypeConfig> columnTypeList = new ArrayList<>(columns.size());
        List<FieldConfig> columnList = new ArrayList<>(columns.size());
        for (int index = 0; index < columns.size(); index++) {
            Column column = columns.get(index);
            String name = column.getName();
            TypeConfig type =
                    TypeConfig.fromString(column.getDataType().getLogicalType().asSummaryString());
            FieldConfig field = new FieldConfig();
            columnNameList.add(name);
            columnTypeList.add(type);
            field.setName(name);
            field.setType(type);
            field.setIndex(index);
            columnList.add(field);
        }
        jdbcConfig.setColumn(columnList);
        jdbcConfig.setMode(
                (CollectionUtil.isNullOrEmpty(jdbcConfig.getUniqueKey()))
                        ? EWriteMode.INSERT.name()
                        : EWriteMode.UPDATE.name());
        jdbcConfig.setPreSql(dorisConfig.getPreSql());
        jdbcConfig.setPostSql(dorisConfig.getPostSql());

        builder.setColumnNameList(columnNameList);
        builder.setColumnTypeList(columnTypeList);

        builder.setConfig(dorisConfig);
        builder.setJdbcDialect(jdbcDialect);
        builder.setJdbcConf(jdbcConfig);
        builder.setRowConverter(new DorisJdbcSqlConverter(rowType));
        setKeyRowConverter(builder, rowType);
        return builder;
    }

    @Override
    public DynamicTableSink copy() {
        return new DorisDynamicTableSink(physicalSchema, dorisConfig);
    }

    @Override
    public String asSummaryString() {
        return "doris sink";
    }
}
