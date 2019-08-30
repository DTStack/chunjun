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
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.ftp.FtpConfigConstants.*;
import static com.dtstack.flinkx.ftp.FtpConfigKeys.*;

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
    private Integer timeout;
    protected long maxFileSize;

    public FtpWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        host = writerConfig.getParameter().getStringVal(KEY_HOST);
        protocol = writerConfig.getParameter().getStringVal(KEY_PROTOCOL, DEFAULT_FTP_PROTOCOL);

        if(SFTP_PROTOCOL.equalsIgnoreCase(protocol)) {
            port = writerConfig.getParameter().getIntVal(KEY_PORT, DEFAULT_SFTP_PORT);
        } else {
            port = writerConfig.getParameter().getIntVal(KEY_PORT, DEFAULT_FTP_PORT);
        }

        username = writerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = writerConfig.getParameter().getStringVal(KEY_PASSWORD);
        writeMode = writerConfig.getParameter().getStringVal(KEY_WRITE_MODE);
        encoding = writerConfig.getParameter().getStringVal(KEY_ENCODING);
        connectPattern = writerConfig.getParameter().getStringVal(KEY_CONNECT_PATTERN, DEFAULT_FTP_CONNECT_PATTERN);
        path = writerConfig.getParameter().getStringVal(KEY_PATH);
        timeout = writerConfig.getParameter().getIntVal(KEY_TIMEOUT, FtpConfigConstants.DEFAULT_TIMEOUT);
        maxFileSize = writerConfig.getParameter().getLongVal(KEY_MAX_FILE_SIZE, 1024 * 1024 * 1024);

        fieldDelimiter = writerConfig.getParameter().getStringVal(KEY_FIELD_DELIMITER, DEFAULT_FIELD_DELIMITER);
        if(!fieldDelimiter.equals(DEFAULT_FIELD_DELIMITER)) {
            fieldDelimiter = StringUtil.convertRegularExpr(fieldDelimiter);
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
        builder.setCharSetName(encoding);
        builder.setErrors(errors);
        builder.setHost(host);
        builder.setConnectPattern(connectPattern);
        builder.setWriteMode(writeMode);
        builder.setMaxFileSize(maxFileSize);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setTimeout(timeout);
        builder.setRestoreConfig(restoreConfig);

        return createOutput(dataSet, builder.finish(), "ftpwriter");
    }
}
