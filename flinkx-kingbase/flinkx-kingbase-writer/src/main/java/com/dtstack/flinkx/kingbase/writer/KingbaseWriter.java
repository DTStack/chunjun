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

package com.dtstack.flinkx.kingbase.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.kingbase.util.KingBaseDatabaseMeta;
import com.dtstack.flinkx.kingbase.util.KingBaseTypeConverter;
import com.dtstack.flinkx.kingbase.format.KingbaseOutputFormat;
import com.dtstack.flinkx.rdb.datawriter.JdbcDataWriter;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.streaming.api.functions.sink.DtOutputFormatSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import static com.dtstack.flinkx.constants.ConfigConstant.KEY_WRITER;

/**
 * KingBase writer plugin
 *
 * Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

public class KingbaseWriter extends JdbcDataWriter {

    public String schema;

    public KingbaseWriter(DataTransferConfig config) {
        super(config);
        schema = config.getJob().getContent().get(0).getWriter().getParameter().getConnection().get(0).getSchema();
        setDatabaseInterface(new KingBaseDatabaseMeta());
        setTypeConverterInterface(new KingBaseTypeConverter());
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        KingbaseOutputFormat kingBaseOutputFormat = new KingbaseOutputFormat();
        kingBaseOutputFormat.setSchema(schema);
        JdbcOutputFormatBuilder builder = new JdbcOutputFormatBuilder(kingBaseOutputFormat);
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
        String sinkName = (databaseInterface.getDatabaseType() + KEY_WRITER).toLowerCase();
        dataStreamSink.name(sinkName);
        return dataStreamSink;
    }
}
