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

package com.dtstack.chunjun.connector.ftp.config;

import com.dtstack.chunjun.config.BaseFileConfig;
import com.dtstack.chunjun.connector.ftp.enums.FileType;
import com.dtstack.chunjun.constants.ConstantValue;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

import static com.dtstack.chunjun.connector.ftp.config.ConfigConstants.DEFAULT_FTP_PORT;
import static com.dtstack.chunjun.connector.ftp.config.ConfigConstants.DEFAULT_SFTP_PORT;
import static com.dtstack.chunjun.connector.ftp.config.ConfigConstants.SFTP_PROTOCOL;

@EqualsAndHashCode(callSuper = true)
@Data
public class FtpConfig extends BaseFileConfig {

    private static final long serialVersionUID = -7859487675736582592L;

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

    /** 如果文件大小大于这个值, 开启文件切割 * */
    private long maxFetchSize = ConstantValue.STORE_SIZE_G;

    private String ftpFileName;

    /** 批量写入数据太大，会导致ftp协议缓冲区报错, 批量写入默认值设置小点 */
    private long nextCheckRows = 100;

    public String encoding = "UTF-8";

    /** 空值替换 */
    public Object nullIsReplacedWithValue = null;

    /** file config * */
    public Map<String, Object> fileConfig;

    /** User defined format class name */
    private String customFormatClassName;

    /** User defined split class name */
    private String customConcurrentFileSplitClassName;

    /* 行分隔符 */
    private String columnDelimiter = "\n";

    /** Get the specified fileReadClient according to the filetype * */
    public String fileType = FileType.TXT.name();

    /** 压缩格式 * */
    public String compressType;

    public void setDefaultPort() {
        if (SFTP_PROTOCOL.equalsIgnoreCase(protocol)) {
            port = DEFAULT_SFTP_PORT;
        } else {
            port = DEFAULT_FTP_PORT;
        }
    }

    /** 工作表 */
    public List<Integer> sheetNo;

    /** 字段对应的列索引 */
    public List<Integer> columnIndex;
}
