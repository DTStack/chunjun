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

package com.dtstack.flinkx.greenplum;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.postgresql.PostgresqlTypeConverter;
import com.dtstack.flinkx.rdb.datawriter.JdbcDataWriter;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.streaming.api.functions.sink.DtOutputFormatSinkFunction;
import com.dtstack.greenplum.GreenplumDatabaseMeta;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

/**
 * The writer plugin for Greenplum database
 *
 * @Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

public class GreenplumWriter extends JdbcDataWriter {

    public GreenplumWriter(DataTransferConfig config) {
        super(config);
        setDatabaseInterface(new GreenplumDatabaseMeta());
        setTypeConverterInterface(new PostgresqlTypeConverter());
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        GreenplumOutputFormat greenplumOutputFormat = new GreenplumOutputFormat();
        JdbcOutputFormatBuilder builder = new JdbcOutputFormatBuilder(greenplumOutputFormat);
        builder.setDriverName(databaseInterface.getDriverClass());
        builder.setDbUrl(dbUrl);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setBatchInterval(batchSize);
        builder.setMonitorUrls(monitorUrls);
        builder.setPreSql(preSql);
        builder.setPostSql(postSql);
        builder.setErrors(errors);
        builder.setErrorRatio(errorRatio);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setDatabaseInterface(databaseInterface);
        builder.setMode(mode);
        builder.setTable(table);
        builder.setColumn(column);
        builder.setFullColumn(fullColumn);
        builder.setUpdateKey(updateKey);
        builder.setTypeConverter(typeConverter);
        builder.setRestoreConfig(restoreConfig);
        builder.setInsertSqlMode(insertSqlMode);

        DtOutputFormatSinkFunction sinkFunction = new DtOutputFormatSinkFunction(builder.finish());
        DataStreamSink<?> dataStreamSink = dataSet.addSink(sinkFunction);
        String sinkName = (databaseInterface.getDatabaseType() + "writer").toLowerCase();
        dataStreamSink.name(sinkName);
        return dataStreamSink;
    }
}
