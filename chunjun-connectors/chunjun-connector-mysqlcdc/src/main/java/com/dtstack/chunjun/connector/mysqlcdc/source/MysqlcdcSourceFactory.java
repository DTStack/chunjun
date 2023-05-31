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

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.chunjun.connector.jdbc.config.ConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.config.SourceConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.chunjun.connector.mysqlcdc.config.MysqlCdcConfig;
import com.dtstack.chunjun.connector.mysqlcdc.converter.MysqlCdcRawTypeMapper;
import com.dtstack.chunjun.converter.RawTypeMapper;
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

    protected MysqlCdcConfig config;

    public MysqlcdcSourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env);
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                ConnectionConfig.class,
                                new ConnectionAdapter(SourceConnectionConfig.class.getName()))
                        .addDeserializationExclusionStrategy(
                                new FieldNameExclusionStrategy("column"))
                        .create();
        GsonUtil.setTypeAdapter(gson);
        config = gson.fromJson(gson.toJson(syncConfig.getReader().getParameter()), getConfClass());
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return MysqlCdcRawTypeMapper::apply;
    }

    @Override
    public DataStream<RowData> createSource() {

        List<DataTypes.Field> dataTypes =
                new ArrayList<>(syncConfig.getReader().getFieldList().size());

        syncConfig
                .getReader()
                .getFieldList()
                .forEach(
                        fieldConfig -> {
                            dataTypes.add(
                                    DataTypes.FIELD(
                                            fieldConfig.getName(),
                                            getRawTypeMapper().apply(fieldConfig.getType())));
                        });
        final DataType dataType = DataTypes.ROW(dataTypes.toArray(new DataTypes.Field[0]));

        DebeziumSourceFunction<RowData> mySqlSource =
                new MySQLSource.Builder<RowData>()
                        .hostname(config.getHost())
                        .port(config.getPort())
                        .databaseList(config.getDatabaseList().toArray(new String[0]))
                        .tableList(
                                config.getTableList()
                                        .toArray(new String[config.getDatabaseList().size()]))
                        .username(config.getUsername())
                        .password(config.getPassword())
                        .serverId(config.getServerId())
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

    protected Class<? extends MysqlCdcConfig> getConfClass() {
        return MysqlCdcConfig.class;
    }

    public static final class DemoValueValidator
            implements RowDataDebeziumDeserializeSchema.ValueValidator {

        private static final long serialVersionUID = -6090345002104410773L;

        @Override
        public void validate(RowData rowData, RowKind rowKind) {
            // do nothing
        }
    }
}
