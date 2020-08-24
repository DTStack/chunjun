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

package com.dtstack.flinkx.ftp.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.ftp.FtpConfig;
import com.dtstack.flinkx.ftp.FtpConfigConstants;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * The reader plugin of Ftp
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpReader extends BaseDataReader {

    private List<MetaColumn> metaColumns;
    private FtpConfig ftpConfig;

    public FtpReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

        try {
            ftpConfig = objectMapper.readValue(objectMapper.writeValueAsString(readerConfig.getParameter().getAll()), FtpConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("解析ftpConfig配置出错:", e);
        }

        if (ftpConfig.getPort() == null) {
            ftpConfig.setDefaultPort();
        }

        if(!FtpConfigConstants.DEFAULT_FIELD_DELIMITER.equals(ftpConfig.getFieldDelimiter())){
            String fieldDelimiter = StringUtil.convertRegularExpr(ftpConfig.getFieldDelimiter());
            ftpConfig.setFieldDelimiter(fieldDelimiter);
        }

        List columns = readerConfig.getParameter().getColumn();
        metaColumns = MetaColumn.getMetaColumns(columns, false);
    }

    @Override
    public DataStream<Row> readData() {
        FtpInputFormatBuilder builder = new FtpInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setFtpConfig(ftpConfig);
        builder.setMetaColumn(metaColumns);
        builder.setTestConfig(testConfig);
        builder.setLogConfig(logConfig);

        return createInput(builder.finish());
    }
}
