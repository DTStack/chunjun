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

package com.dtstack.flinkx.mongodb.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.mongodb.MongodbConfig;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * The writer plugin for mongodb database
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class MongodbWriter extends BaseDataWriter {

    protected List<MetaColumn> columns;

    protected MongodbConfig mongodbConfig;

    public MongodbWriter(DataTransferConfig config) {
        super(config);

        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        columns = MetaColumn.getMetaColumns(writerConfig.getParameter().getColumn());

        try {
            mongodbConfig = objectMapper.readValue(objectMapper.writeValueAsString(writerConfig.getParameter().getAll()), MongodbConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("解析mongodb配置出错:", e);
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        MongodbOutputFormatBuilder builder = new MongodbOutputFormatBuilder();

        builder.setMongodbConfig(mongodbConfig);
        builder.setColumns(columns);
        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);

        return createOutput(dataSet, builder.finish());
    }
}
