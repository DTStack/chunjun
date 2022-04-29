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

package com.dtstack.chunjun.connector.ftp.client;

import com.dtstack.chunjun.connector.ftp.conf.FtpConfig;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

public abstract class AbstractFileReader implements FileReadClient {

    private File file;

    @Override
    public void open(File file, InputStream inputStream, FtpConfig ftpConfig) throws IOException {
        this.file = file;
        if (StringUtils.isNotBlank(ftpConfig.getCompressType())) {
            InputStream compressStream =
                    getCompressStream(inputStream, ftpConfig.getCompressType());
            open(compressStream, ftpConfig);
        } else {
            open(inputStream, ftpConfig);
        }
    }

    public abstract void open(InputStream inputStream, FtpConfig ftpConfig) throws IOException;

    public InputStream getCompressStream(InputStream inputStream, String compressType) {
        switch (compressType.toUpperCase(Locale.ENGLISH)) {
            case "ZIP":
                ZipInputStream zipInputStream = new ZipInputStream(inputStream);
                zipInputStream.addFileName(file.getFileName());
                return zipInputStream;
            default:
                throw new RuntimeException("not support " + compressType);
        }
    }

    public String getFileName() {
        return file.getFileName();
    }
}
