/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.mysqlcdc.source;

import com.dtstack.chunjun.config.SyncConf;
import com.dtstack.chunjun.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.chunjun.connector.jdbc.config.CdcConf;
import com.dtstack.chunjun.connector.jdbc.config.ConnectionConf;
import com.dtstack.chunjun.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.chunjun.connector.mysqlcdc.converter.MysqlCdcRawTypeConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class MysqlcdcSourceFactory extends SourceFactory {

    protected CdcConf cdcConf;

    public MysqlcdcSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                ConnectionConf.class, new ConnectionAdapter("SourceConnectionConf"))
                        .addDeserializationExclusionStrategy(
                                new FieldNameExclusionStrategy("column"))
                        .create();
        GsonUtil.setTypeAdapter(gson);
        cdcConf = gson.fromJson(gson.toJson(syncConf.getReader().getParameter()), getConfClass());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return MysqlCdcRawTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {

        List<DataTypes.Field> dataTypes =
                new ArrayList<>(syncConf.getReader().getFieldList().size());

        syncConf.getReader()
                .getFieldList()
                .forEach(
                        fieldConf -> {
                            dataTypes.add(
                                    DataTypes.FIELD(
                                            fieldConf.getName(),
                                            getRawTypeConverter().apply(fieldConf.getType())));
                        });
        final DataType dataType = DataTypes.ROW(dataTypes.toArray(new DataTypes.Field[0]));

        DebeziumSourceFunction<RowData> mySqlSource =
                new MySQLSource.Builder<RowData>()
                        .hostname(cdcConf.getHost())
                        .port(cdcConf.getPort())
                        .databaseList(cdcConf.getDatabaseList().toArray(new String[0]))
                        .tableList(
                                cdcConf.getTableList()
                                        .toArray(new String[cdcConf.getDatabaseList().size()]))
                        .username(cdcConf.getUsername())
                        .password(cdcConf.getPassword())
                        .serverId(cdcConf.getServerId())
                        .deserializer(buildRowDataDebeziumDeserializeSchema(dataType))
                        .build();

        return env.addSource(mySqlSource, "MysqlCdcSource", getTypeInformation());
    }

    private DebeziumDeserializationSchema<RowData> buildRowDataDebeziumDeserializeSchema(
            DataType dataType) {
        return new RowDataDebeziumDeserializeSchema(
                (RowType) dataType.getLogicalType(),
                getTypeInformation(),
                new DemoValueValidator(),
                ZoneOffset.UTC);
    }

    protected Class<? extends CdcConf> getConfClass() {
        return CdcConf.class;
    }

    public static final class DemoValueValidator
            implements RowDataDebeziumDeserializeSchema.ValueValidator {

        @Override
        public void validate(RowData rowData, RowKind rowKind) {
            // do nothing
        }
    }
}
