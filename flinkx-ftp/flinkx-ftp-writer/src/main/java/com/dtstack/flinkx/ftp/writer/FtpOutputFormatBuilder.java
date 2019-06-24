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

import com.dtstack.flinkx.ftp.FtpConfigConstants;
import com.dtstack.flinkx.outputformat.RichOutputFormatBuilder;
import org.apache.commons.lang.StringUtils;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;

/**
 * The builder of FtpOutputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpOutputFormatBuilder extends RichOutputFormatBuilder {

    private FtpOutputFormat format;

    public FtpOutputFormatBuilder() {
        super.format = format = new FtpOutputFormat();
    }

    public void setProtocol(String protocol) {
        format.protocol = protocol;
    }

    public void setPath(String path) {
        if(StringUtils.isEmpty(path)) {
            format.path = "/";
        } else {
            format.path = path;
        }
    }

    public void setHost(String host) {
        format.host = host;
    }

    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setPort(Integer port) {
        format.port = port;
    }

    public void setColumnNames(List<String> columnNames) {
        format.columnNames = columnNames;
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.columnTypes = columnTypes;
    }

    public void setDelimiter(String delimiter) {
        format.delimiter = delimiter;
    }

    public void setEncoding(String charsetName) {
        if(StringUtils.isNotEmpty(charsetName)) {
            if(!Charset.isSupported(charsetName)) {
                throw new UnsupportedCharsetException("The charset " + charsetName + " is not supported.");
            }
            this.format.charsetName = charsetName;
        }
    }

    public void setWriteMode (String writeMode) {
        format.writeMode = writeMode;
    }

    public void setConnectPattern(String connectPattern) {
        if(StringUtils.isEmpty(connectPattern)) {
            format.connectMode = FtpConfigConstants.DEFAULT_FTP_CONNECT_PATTERN;
        } else {
            format.connectMode = connectPattern;
        }
    }

    public void setTimeout(Integer timeout){
        format.timeout = timeout;
    }

    @Override
    protected void checkFormat() {

    }

}
