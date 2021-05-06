/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.dtstack.flinkx.ftp;

import com.dtstack.flinkx.constants.ConstantValue;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.io.Serializable;

import static com.dtstack.flinkx.ftp.FtpConfigConstants.SFTP_PROTOCOL;

/**
 * @author jiangbo
 * @date 2019/12/9
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FtpConfig implements Serializable {

    public String username;

    public String password;

    public String privateKeyPath;

    public String protocol;

    public String fieldDelimiter = FtpConfigConstants.DEFAULT_FIELD_DELIMITER;

    public String path = "/";

    public String encoding = "UTF-8";

    public String connectPattern = FtpConfigConstants.STANDARD_FTP_PROTOCOL;

    public String host;

    public Integer port;

    public String writeMode;

    public boolean isFirstLineHeader = false;

    public int timeout = FtpConfigConstants.DEFAULT_TIMEOUT;

    public long maxFileSize = 1024 * 1024 * 1024L;

    /** ftp客户端编码格式 **/
    public String controlEncoding = System.getProperty(ConstantValue.SYSTEM_PROPERTIES_KEY_FILE_ENCODING);

    /** linux是否展示隐藏文件 **/
    public boolean listHiddenFiles = true;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPrivateKeyPath() {
        return privateKeyPath;
    }

    public void setPrivateKeyPath(String privateKeyPath) {
        this.privateKeyPath = privateKeyPath;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getConnectPattern() {
        return connectPattern;
    }

    public void setConnectPattern(String connectPattern) {
        this.connectPattern = connectPattern;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public void setDefaultPort(){
        if (SFTP_PROTOCOL.equalsIgnoreCase(protocol)) {
            port = FtpConfigConstants.DEFAULT_SFTP_PORT;
        } else {
            port = FtpConfigConstants.DEFAULT_FTP_PORT;
        }
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public boolean getIsFirstLineHeader() {
        return isFirstLineHeader;
    }

    public void setIsFirstLineHeader(boolean isFirstLineHeader) {
        this.isFirstLineHeader = isFirstLineHeader;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public String getControlEncoding() {
        return controlEncoding;
    }

    public void setControlEncoding(String controlEncoding) {
        this.controlEncoding = controlEncoding;
    }

    public boolean getListHiddenFiles() {
        return listHiddenFiles;
    }

    public void setListHiddenFiles(boolean listHiddenFiles) {
        this.listHiddenFiles = listHiddenFiles;
    }
}
