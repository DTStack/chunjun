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

package com.dtstack.chunjun.connector.elasticsearch7;

import java.io.Serializable;
import java.util.Map;

public class SslConf implements Serializable {
    private static final long serialVersionUID = 1L;

    /** whether to use local files */
    private boolean useLocalFile;

    /** SSL file name * */
    private String fileName;
    /** SSL Folder Path * */
    private String filePath;
    /** Ssl file Password * */
    private String keyStorePass = "";
    /** ssl type pkcs12 or ca * */
    private String type = "pkcs12";
    /** sftp config * */
    private Map<String, Object> sftpConf;

    public boolean isUseLocalFile() {
        return useLocalFile;
    }

    public void setUseLocalFile(boolean useLocalFile) {
        this.useLocalFile = useLocalFile;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getKeyStorePass() {
        return keyStorePass;
    }

    public void setKeyStorePass(String keyStorePass) {
        this.keyStorePass = keyStorePass;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getSftpConf() {
        return sftpConf;
    }

    public void setSftpConf(Map<String, Object> sftpConf) {
        this.sftpConf = sftpConf;
    }
}
