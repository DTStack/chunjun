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
import com.dtstack.flinkx.ftp.FtpConfigConstants;
import com.dtstack.flinkx.ftp.FtpConfigKeys;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Writer Plugin of Ftp
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpWriter extends DataWriter{

    private String protocol;
    private String host;
    private int port;
    private String username;
    private String password;
    private String writeMode;
    private String encoding;
    private String connectPattern;
    private String path;
    private String fieldDelimiter;

    private List<String> columnName;
    private List<String> columnType;

    public FtpWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        this.protocol = writerConfig.getParameter().getStringVal(FtpConfigKeys.KEY_PROTOCOL);
        this.host = writerConfig.getParameter().getStringVal(FtpConfigKeys.KEY_HOST);
        if(FtpConfigConstants.SFTP_PROTOCOL.equalsIgnoreCase(protocol)) {
            port = writerConfig.getParameter().getIntVal(FtpConfigKeys.KEY_PORT, FtpConfigConstants.DEFAULT_SFTP_PORT);
        } else {
            port = writerConfig.getParameter().getIntVal(FtpConfigKeys.KEY_PORT, FtpConfigConstants.DEFAULT_FTP_PORT);
        }
        this.username = writerConfig.getParameter().getStringVal(FtpConfigKeys.KEY_USERNAME);
        this.password = writerConfig.getParameter().getStringVal(FtpConfigKeys.KEY_PASSWORD);
        this.writeMode = writerConfig.getParameter().getStringVal(FtpConfigKeys.KEY_WRITE_MODE);
        this.encoding = writerConfig.getParameter().getStringVal(FtpConfigKeys.KEY_ENCODING);
        this.connectPattern = writerConfig.getParameter().getStringVal(FtpConfigKeys.KEY_CONNECT_PATTERN, FtpConfigConstants.DEFAULT_FTP_CONNECT_PATTERN);
        this.path = writerConfig.getParameter().getStringVal(FtpConfigKeys.KEY_PATH);
        this.fieldDelimiter = writerConfig.getParameter().getStringVal(FtpConfigKeys.KEY_FIELD_DELIMITER);

        if(fieldDelimiter == null || fieldDelimiter.length() == 0) {
            fieldDelimiter = "\001";
        } else {
            String pattern = "\\\\(\\d{3})";

            Pattern r = Pattern.compile(pattern);
            while(true) {
                Matcher m = r.matcher(fieldDelimiter);
                if(!m.find()) {
                    break;
                }
                String num = m.group(1);
                int x = Integer.parseInt(num, 8);
                fieldDelimiter = m.replaceFirst(String.valueOf((char)x));
            }
            fieldDelimiter = fieldDelimiter.replaceAll("\\\\t","\t");
            fieldDelimiter = fieldDelimiter.replaceAll("\\\\r","\r");
            fieldDelimiter = fieldDelimiter.replaceAll("\\\\n","\n");
        }

        List columns = writerConfig.getParameter().getColumn();
        if(columns != null || columns.size() != 0) {
            columnName = new ArrayList<>();
            columnType = new ArrayList<>();
            for(int i = 0; i < columns.size(); ++i) {
                Map sm = (Map) columns.get(i);
                columnName.add((String) sm.get("name"));
                columnType.add((String) sm.get("type"));
            }
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        FtpOutputFormatBuilder builder = new FtpOutputFormatBuilder();
        builder.setProtocol(protocol);
        builder.setMonitorUrls(monitorUrls);
        builder.setPort(port);
        builder.setPath(path);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setColumnNames(columnName);
        builder.setColumnTypes(columnType);
        builder.setDelimiter(fieldDelimiter);
        builder.setEncoding(encoding);
        builder.setErrors(errors);
        builder.setHost(host);
        builder.setConnectPattern(connectPattern);
        builder.setWriteMode(writeMode);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);

        OutputFormatSinkFunction sinkFunction = new OutputFormatSinkFunction(builder.finish());
        DataStreamSink<?> dataStreamSink = dataSet.addSink(sinkFunction);

        dataStreamSink.name("ftpwriter");

        return dataStreamSink;
    }
}
