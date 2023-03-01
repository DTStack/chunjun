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
import com.dtstack.chunjun.connector.ftp.handler.IFtpHandler;
import com.dtstack.chunjun.connector.ftp.source.FtpFileSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

    public static void addCompressFile(
            IFtpHandler ftpHandler,
            String filePath,
            FtpConfig ftpConfig,
            List<FtpFileSplit> fileList)
            throws IOException {
        if ("ZIP".equals(ftpConfig.getCompressType().toUpperCase(Locale.ENGLISH))) {
            try (ZipInputStream zipInputStream =
                    new ZipInputStream(
                            ftpHandler.getInputStream(filePath),
                            Charset.forName(ftpConfig.encoding))) {
                ZipEntry zipEntry;
                while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                    fileList.add(
                            new FtpFileSplit(
                                    0,
                                    zipEntry.getSize(),
                                    filePath,
                                    zipEntry.getName(),
                                    ftpConfig.getCompressType()));
                }
                closeWithFtpHandler(ftpHandler, LOG);
            }
        } else {
            throw new RuntimeException("not support compressType " + ftpConfig.getCompressType());
        }
    }

    public static void closeWithFtpHandler(IFtpHandler ftpHandler, Logger log) {
        if (ftpHandler instanceof FtpHandler) {
            try {
                ((FtpHandler) ftpHandler).getFtpClient().completePendingCommand();
            } catch (Exception e) {
                log.warn("FTPClient completePendingCommand has error ->", e);
                try {
                    ftpHandler.logoutFtpServer();
                } catch (Exception exception) {
                    log.warn("FTPClient logout has error ->", exception);
                }
            }
        }
    }

    public static String getFilename(String filepath) {
        String[] paths = filepath.split("/");
        return paths[paths.length - 1];
    }

    /** analyse file */
    public static void addFile(
            IFtpHandler ftpHandler,
            String filePath,
            FtpConfig ftpConfig,
            List<FtpFileSplit> fileList)
            throws Exception {
        long maxFetchSize = ftpConfig.getMaxFetchSize();

        // fetchSize should bigger than 1M
        maxFetchSize = Math.max(maxFetchSize, 1024 * 1024);

        long currentFileSize = ftpHandler.getFileSize(filePath);
        int parallelism = ftpConfig.getParallelism();

        String filename = getFilename(filePath);

        // do not split excel
        if (ftpConfig.getFileType() == null
                || ftpConfig.getFileType().equals("excel")
                || ftpConfig.getFileType().equals("custom")) {
            FtpFileSplit ftpFileSplit = new FtpFileSplit(0, currentFileSize, filePath, filename);
            fileList.add(ftpFileSplit);
            return;
        }

        // split file
        if (maxFetchSize < currentFileSize) {
            int perSplit = Math.min((int) currentFileSize / parallelism, Integer.MAX_VALUE);
            long startPosition = 0;
            long endPosition = startPosition + perSplit;

            while (endPosition <= currentFileSize) {
                if (endPosition == currentFileSize) {
                    FtpFileSplit ftpFileSplit =
                            new FtpFileSplit(startPosition, endPosition, filePath, filename);
                    fileList.add(ftpFileSplit);
                    break;
                }

                InputStream input = ftpHandler.getInputStreamByPosition(filePath, endPosition);
                char c = ' ';

                while (c != '\n') {
                    c = (char) input.read();
                    endPosition += 1;
                }
                FtpFileSplit ftpFileSplit =
                        new FtpFileSplit(startPosition, endPosition, filePath, filename);
                fileList.add(ftpFileSplit);

                LOG.info(
                        String.format(
                                "build file split, filename: %s, startPosition: %d, endPosition: %d",
                                filePath, startPosition, endPosition));

                startPosition = endPosition;
                endPosition = startPosition + perSplit;
            }

            if (startPosition != currentFileSize) {
                FtpFileSplit ftpFileSplit =
                        new FtpFileSplit(startPosition, currentFileSize, filePath, filename);
                fileList.add(ftpFileSplit);

                LOG.info(
                        String.format(
                                "build file split, filename: %s, startPosition: %d, endPosition: %d",
                                filePath, startPosition, currentFileSize));
            }
        } else {
            FtpFileSplit ftpFileSplit = new FtpFileSplit(0, currentFileSize, filePath, filename);
            fileList.add(ftpFileSplit);
        }
    }
}
