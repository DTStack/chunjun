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

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.chunjun.connector.jdbc.conf.CdcConf;
import com.dtstack.chunjun.connector.jdbc.conf.ConnectionConf;
import com.dtstack.chunjun.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

public class MysqlCdcSourceFactory extends SourceFactory {

    protected CdcConf cdcConf;

    protected MysqlCdcSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
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
        return null;
    }

    @Override
    public DataStream<RowData> createSource() {
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("description", DataTypes.STRING()),
                        DataTypes.FIELD("weight", DataTypes.FLOAT()));

        MySqlSource<RowData> mySqlSource = new MySqlSourceBuilder<RowData>()
                .hostname(cdcConf.getHost())
                .port(cdcConf.getPort())
                .databaseList(cdcConf.getDatabaseList().toArray(new String[cdcConf.getDatabaseList().size()]))
                .tableList(cdcConf.getTableList().toArray(new String[cdcConf.getDatabaseList().size()]))
                .username(cdcConf.getUserName())
                .password(cdcConf.getPassword())
                .serverId(cdcConf.getServerId())
                .deserializer(buildRowDataDebeziumDeserializeSchema(dataType))
                .includeSchemaChanges(true) // output the schema changes as well
                .splitSize(2)
                .build();

        return env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySqlParallelSource");
    }

    private DebeziumDeserializationSchema<RowData> buildRowDataDebeziumDeserializeSchema(
            DataType dataType) {
        LogicalType logicalType = TypeConversions.fromDataToLogicalType(dataType);
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(logicalType);
        return RowDataDebeziumDeserializeSchema.newBuilder()
                .setPhysicalRowType((RowType) dataType.getLogicalType())
                .setResultTypeInfo(typeInfo)
                .build();
    }

    protected Class<? extends CdcConf> getConfClass() {
        return CdcConf.class;
    }
}
