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
import com.dtstack.flinkx.ftp.FtpConfigConstants;
import com.dtstack.flinkx.reader.DataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import java.util.List;
import static com.dtstack.flinkx.ftp.FtpConfigKeys.*;
import static com.dtstack.flinkx.ftp.FtpConfigConstants.*;

/**
 * The reader plugin of Ftp
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpReader extends DataReader {

    private String protocol;
    private String host;
    private int port;
    private String connectPattern;
    private String username;
    private String password;
    private String path;
    private String fieldDelimiter;
    private String encoding;
    private boolean isFirstLineHeader;
    private List<MetaColumn> metaColumns;
    private Integer timeout;


    public FtpReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        path = readerConfig.getParameter().getStringVal(KEY_PATH);
        host = readerConfig.getParameter().getStringVal(KEY_HOST);
        connectPattern = readerConfig.getParameter().getStringVal(KEY_CONNECT_PATTERN);
        protocol = readerConfig.getParameter().getStringVal(KEY_PROTOCOL, DEFAULT_FTP_PROTOCOL);

        if(SFTP_PROTOCOL.equalsIgnoreCase(protocol)) {
            port = readerConfig.getParameter().getIntVal(KEY_PORT, FtpConfigConstants.DEFAULT_SFTP_PORT);
        } else {
            port = readerConfig.getParameter().getIntVal(KEY_PORT, FtpConfigConstants.DEFAULT_FTP_PORT);
        }

        fieldDelimiter = readerConfig.getParameter().getStringVal(KEY_FIELD_DELIMITER, DEFAULT_FIELD_DELIMITER);
        if(!fieldDelimiter.equals(DEFAULT_FIELD_DELIMITER)) {
            fieldDelimiter = StringUtil.convertRegularExpr(fieldDelimiter);
        }

        username = readerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = readerConfig.getParameter().getStringVal(KEY_PASSWORD);
        encoding = readerConfig.getParameter().getStringVal(KEY_ENCODING);
        isFirstLineHeader = readerConfig.getParameter().getBooleanVal(KEY_IS_FIRST_HEADER,false);
        timeout = readerConfig.getParameter().getIntVal(KEY_TIMEOUT, FtpConfigConstants.DEFAULT_TIMEOUT);

        List columns = readerConfig.getParameter().getColumn();
        metaColumns = MetaColumn.getMetaColumns(columns);
    }

    @Override
    public DataStream<Row> readData() {
        FtpInputFormatBuilder builder = new FtpInputFormatBuilder();
        builder.setMetaColumn(metaColumns);
        builder.setConnectMode(connectPattern);
        builder.setDelimiter(fieldDelimiter);
        builder.setEncoding(encoding);
        builder.setHost(host);
        builder.setPassword(password);
        builder.setPath(path);
        builder.setPort(port);
        builder.setProtocol(protocol);
        builder.setUsername(username);
        builder.setIsFirstLineHeader(isFirstLineHeader);
        builder.setTimeout(timeout);

        return createInput(builder.finish(), "ftpreader");
    }
}
