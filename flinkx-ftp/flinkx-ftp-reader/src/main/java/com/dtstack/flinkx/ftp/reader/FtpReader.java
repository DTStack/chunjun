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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

    private List<Integer> columnIndex;
    private List<String> columnType;
    private List<String> columnValue;


    protected FtpReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        path = readerConfig.getParameter().getStringVal(KEY_PATH);
        host = readerConfig.getParameter().getStringVal(KEY_HOST);
        connectPattern = readerConfig.getParameter().getStringVal(KEY_CONNECT_PATTERN);
        protocol = readerConfig.getParameter().getStringVal(KEY_PROTOCOL);

        if(SFTP_PROTOCOL.equalsIgnoreCase(protocol)) {
            port = readerConfig.getParameter().getIntVal(KEY_PORT, FtpConfigConstants.DEFAULT_SFTP_PORT);
        } else {
            port = readerConfig.getParameter().getIntVal(KEY_PORT, FtpConfigConstants.DEFAULT_FTP_PORT);
        }

        this.fieldDelimiter = readerConfig.getParameter().getStringVal(KEY_FIELD_DELIMITER);
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

        this.username = readerConfig.getParameter().getStringVal(KEY_USERNAME);
        this.password = readerConfig.getParameter().getStringVal(KEY_PASSWORD);
        this.encoding = readerConfig.getParameter().getStringVal(KEY_ENCODING);

        List columns = readerConfig.getParameter().getColumn();
        if(columns != null && columns.size() > 0) {
            if (columns.get(0) instanceof Map) {
                columnIndex = new ArrayList();
                columnType = new ArrayList<>();
                columnValue = new ArrayList<>();
                for (int i = 0; i < columns.size(); ++i) {
                    Map sm = (Map) columns.get(i);
                    Double temp = (Double) sm.get("index");
                    columnIndex.add(temp != null ? temp.intValue() : null);
                    columnType.add((String) sm.get("type"));
                    columnValue.add((String) sm.get("value"));
                }
                System.out.println("init column finished");
            } else if (!columns.get(0).equals("*") || columns.size() != 1) {
                throw new IllegalArgumentException("column argument error");
            }
        }
    }

    @Override
    public DataStream<Row> readData() {
        FtpInputFormatBuilder builder = new FtpInputFormatBuilder();
        builder.setColumnType(columnType);
        builder.setColumnIndex(columnIndex);
        builder.setColumnValue(columnValue);
        builder.setConnectMode(connectPattern);
        builder.setDelimiter(fieldDelimiter);
        builder.setEncoding(encoding);
        builder.setHost(host);
        builder.setPassword(password);
        builder.setPath(path);
        builder.setPort(port);
        builder.setProtocol(protocol);
        builder.setUsername(username);

        return createInput(builder.finish(), "ftpreader");
    }
}
