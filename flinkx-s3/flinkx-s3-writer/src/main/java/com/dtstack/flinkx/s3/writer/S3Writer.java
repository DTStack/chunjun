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

package com.dtstack.flinkx.s3.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.SpeedConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.s3.S3Config;
import com.dtstack.flinkx.s3.format.S3OutputFormatBuilder;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Writer Plugin of S3
 * <p>
 * company www.dtstack.com
 *
 * @author jier
 */
public class S3Writer extends BaseDataWriter {

    private List<String> columnName;
    private List<String> columnType;
    private S3Config s3Config;
    private SpeedConfig speedConfig;

    public S3Writer(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        this.speedConfig = config.getJob().getSetting().getSpeed();
        try {
            String s3json = objectMapper.writeValueAsString(writerConfig.getParameter().getAll());
            s3Config = objectMapper.readValue(s3json, S3Config.class);
        } catch (Exception e) {
            throw new RuntimeException("解析S3Config配置出错:", e);
        }


        List columns = writerConfig.getParameter().getColumn();
        if(columns != null && columns.size() != 0) {
            columnName = new ArrayList<>();
            columnType = new ArrayList<>();
            for(int i = 0; i < columns.size(); ++i) {
                Map sm = (Map) columns.get(i);
                columnName.add(String.valueOf(sm.get("name")));
                columnType.add(String.valueOf(sm.get("type")));
            }
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        S3OutputFormatBuilder builder = new S3OutputFormatBuilder();
        builder.setS3Config(s3Config);
        builder.setSpeedConfig(speedConfig);
        builder.setMonitorUrls(monitorUrls);
        builder.setColumnNames(columnName);
        builder.setColumnTypes(columnType);
        builder.setMaxFileSize(s3Config.getMaxFileSize());
        builder.setErrors(errors);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setRestoreConfig(restoreConfig);

        return createOutput(dataSet, builder.finish());
    }
}
