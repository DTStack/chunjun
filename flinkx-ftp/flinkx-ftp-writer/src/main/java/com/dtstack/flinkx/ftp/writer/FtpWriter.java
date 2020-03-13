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

package com.dtstack.flinkx.ftp.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.ftp.FtpConfig;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.ftp.FtpConfigConstants.*;

/**
 * The Writer Plugin of Ftp
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpWriter extends BaseDataWriter {

    private List<String> columnName;
    private List<String> columnType;
    private FtpConfig ftpConfig;

    public FtpWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();

        try {
            ftpConfig = objectMapper.readValue(objectMapper.writeValueAsString(writerConfig.getParameter().getAll()), FtpConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("解析ftpConfig配置出错:", e);
        }

        if (ftpConfig.getPort() == null) {
            ftpConfig.setDefaultPort();
        }

        if(!DEFAULT_FIELD_DELIMITER.equals(ftpConfig.getFieldDelimiter())){
            String fieldDelimiter = StringUtil.convertRegularExpr(ftpConfig.getFieldDelimiter());
            ftpConfig.setFieldDelimiter(fieldDelimiter);
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
        FtpOutputFormatBuilder builder = new FtpOutputFormatBuilder();
        builder.setFtpConfig(ftpConfig);
        builder.setPath(ftpConfig.getPath());
        builder.setMaxFileSize(ftpConfig.getMaxFileSize());
        builder.setMonitorUrls(monitorUrls);
        builder.setColumnNames(columnName);
        builder.setColumnTypes(columnType);
        builder.setErrors(errors);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setRestoreConfig(restoreConfig);

        return createOutput(dataSet, builder.finish());
    }
}
