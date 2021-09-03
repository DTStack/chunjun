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

package com.dtstack.flinkx.connector.ftp.conf;

import com.dtstack.flinkx.conf.BaseFileConf;
import com.dtstack.flinkx.constants.ConstantValue;

/**
 * @author jiangbo
 * @date 2019/12/9
 */
public class FtpConfig extends BaseFileConf {

    public Integer timeout = ConfigConstants.DEFAULT_TIMEOUT;
    private String username;
    private String password;
    private String privateKeyPath;
    private String protocol;
    private String fieldDelimiter = ConfigConstants.DEFAULT_FIELD_DELIMITER;
    private String connectPattern = ConfigConstants.DEFAULT_FTP_CONNECT_PATTERN;
    private String host;
    private Integer port;
    private boolean isFirstLineHeader = false;
    /** ftp客户端编码格式 * */
    private String controlEncoding =
            System.getProperty(ConstantValue.SYSTEM_PROPERTIES_KEY_FILE_ENCODING);
    /** linux是否展示隐藏文件 * */
    private boolean listHiddenFiles = true;

    private String ftpFileName;

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

    public void setDefaultPort() {
        if (ConfigConstants.SFTP_PROTOCOL.equalsIgnoreCase(protocol)) {
            port = ConfigConstants.DEFAULT_SFTP_PORT;
        } else {
            port = ConfigConstants.DEFAULT_FTP_PORT;
        }
    }

    public boolean getIsFirstLineHeader() {
        return isFirstLineHeader;
    }

    public void setIsFirstLineHeader(boolean isFirstLineHeader) {
        this.isFirstLineHeader = isFirstLineHeader;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public boolean isFirstLineHeader() {
        return isFirstLineHeader;
    }

    public void setFirstLineHeader(boolean firstLineHeader) {
        isFirstLineHeader = firstLineHeader;
    }

    public String getControlEncoding() {
        return controlEncoding;
    }

    public void setControlEncoding(String controlEncoding) {
        this.controlEncoding = controlEncoding;
    }

    public boolean isListHiddenFiles() {
        return listHiddenFiles;
    }

    public void setListHiddenFiles(boolean listHiddenFiles) {
        this.listHiddenFiles = listHiddenFiles;
    }

    public String getFtpFileName() {
        return ftpFileName;
    }

    public void setFtpFileName(String ftpFileName) {
        this.ftpFileName = ftpFileName;
    }
}
