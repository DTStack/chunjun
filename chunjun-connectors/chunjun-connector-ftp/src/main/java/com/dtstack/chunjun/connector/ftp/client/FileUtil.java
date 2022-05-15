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
import com.dtstack.chunjun.connector.ftp.handler.FtpHandler;
import com.dtstack.chunjun.connector.ftp.handler.FtpHandlerFactory;
import com.dtstack.chunjun.connector.ftp.handler.IFtpHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

    public static void addFile(
            IFtpHandler ftpHandler, String filePath, FtpConfig ftpConfig, List<File> fileList)
            throws IOException {
        switch (ftpConfig.getCompressType().toUpperCase(Locale.ENGLISH)) {
            case "ZIP":
                try (java.util.zip.ZipInputStream zipInputStream =
                        new ZipInputStream(
                                ftpHandler.getInputStream(filePath),
                                Charset.forName(ftpConfig.encoding))) {
                    ZipEntry zipEntry;
                    while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                        fileList.add(
                                new File(
                                        filePath,
                                        filePath + "/" + zipEntry.getName(),
                                        zipEntry.getName(),
                                        ftpConfig.getCompressType()));
                    }
                    if (ftpHandler instanceof FtpHandler) {
                        try {
                            ((FtpHandler) ftpHandler).getFtpClient().completePendingCommand();
                        } catch (Exception e) {
                            // 如果出现了超时异常，就直接获取一个新的ftpHandler
                            LOG.warn("FTPClient completePendingCommand has error ->", e);
                            try {
                                ftpHandler.logoutFtpServer();
                            } catch (Exception exception) {
                                LOG.warn("FTPClient logout has error ->", exception);
                            }
                            ftpHandler =
                                    FtpHandlerFactory.createFtpHandler(ftpConfig.getProtocol());
                            ftpHandler.loginFtpServer(ftpConfig);
                        }
                    }
                }
                break;
            default:
                throw new RuntimeException(
                        "not support compressType " + ftpConfig.getCompressType());
        }
    }
}
