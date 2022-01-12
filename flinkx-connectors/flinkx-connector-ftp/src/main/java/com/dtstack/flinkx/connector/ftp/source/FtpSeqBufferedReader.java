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

package com.dtstack.flinkx.connector.ftp.source;

import com.dtstack.flinkx.connector.ftp.client.Data;
import com.dtstack.flinkx.connector.ftp.client.File;
import com.dtstack.flinkx.connector.ftp.client.FileReadClient;
import com.dtstack.flinkx.connector.ftp.client.FileReadClientFactory;
import com.dtstack.flinkx.connector.ftp.conf.FtpConfig;
import com.dtstack.flinkx.connector.ftp.handler.FtpHandler;
import com.dtstack.flinkx.connector.ftp.handler.FtpHandlerFactory;
import com.dtstack.flinkx.connector.ftp.handler.IFtpHandler;
import com.dtstack.flinkx.connector.ftp.handler.Position;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;

/**
 * The FtpSeqBufferedReader that read multiple ftp files one by one.
 *
 * @author jiangbo
 * @date 2018/12/18
 */
public class FtpSeqBufferedReader {

    private static Logger LOG = LoggerFactory.getLogger(FtpSeqBufferedReader.class);

    private IFtpHandler ftpHandler;

    private Iterator<File> iter;

    private int fromLine = 0;

    private FileReadClient fileReadClient;

    private File currentFile;

    private Long currentFileReadLineNum = 0L;

    private Position startPosition;

    // ftp配置信息
    private FtpConfig ftpConfig;

    public FtpSeqBufferedReader(
            IFtpHandler ftpHandler,
            Iterator<File> iter,
            FtpConfig ftpConfig,
            Position startPosition) {
        this.ftpHandler = ftpHandler;
        this.iter = iter;
        this.ftpConfig = ftpConfig;
        this.startPosition = startPosition;
    }

    public Data readLine() throws IOException {
        if (fileReadClient == null) {
            nextStream();
        }

        if (fileReadClient != null) {
            if (!fileReadClient.hasNext()) {
                close();
                return readLine();
            }
            currentFileReadLineNum++;
            return new Data(
                    fileReadClient.nextRecord(), new Position(currentFileReadLineNum, currentFile));
        } else {
            return null;
        }
    }

    private void nextStream() throws IOException {
        if (iter.hasNext()) {
            File file = iter.next();
            InputStream in = null;
            if (file.getFileCompressPath() != null) {
                in = ftpHandler.getInputStream(file.getFileCompressPath());
            } else {
                in = ftpHandler.getInputStream(file.getFileAbsolutePath());
            }

            if (in == null) {
                throw new RuntimeException(
                        String.format(
                                "can not get inputStream for file [%s], please check file read and write permissions",
                                file));
            }

            fileReadClient =
                    FileReadClientFactory.create(
                            file.getFileAbsolutePath(), ftpConfig.getFileType());
            fileReadClient.open(file, in, ftpConfig);
            currentFile = file;
            currentFileReadLineNum = 0L;
            for (int i = 0; i < fromLine; i++) {
                if (fileReadClient.hasNext()) {
                    String[] strings = fileReadClient.nextRecord();
                    LOG.info("Skip line:{}", Arrays.toString(strings));
                } else {
                    break;
                }
            }

            // 从续跑时恢复需要过滤已经读取的数据
            if (startPosition != null
                    && startPosition.getLine() > 0
                    && startPosition
                            .getFile()
                            .getFileAbsolutePath()
                            .equals(file.getFileAbsolutePath())) {
                LOG.info("start skip  [{}]  number line", startPosition.getLine());
                for (int i = 0; i < startPosition.getLine(); i++) {
                    if (fileReadClient.hasNext()) {
                        String[] strings = fileReadClient.nextRecord();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Skip line:{}", Arrays.toString(strings));
                        }
                    } else {
                        break;
                    }
                }
                startPosition = null;
            }

        } else {
            fileReadClient = null;
        }
    }

    public void close() throws IOException {
        if (fileReadClient != null) {
            fileReadClient.close();
            fileReadClient = null;

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
                    ftpHandler = FtpHandlerFactory.createFtpHandler(ftpConfig.getProtocol());
                    ftpHandler.loginFtpServer(ftpConfig);
                }
            }
        }
    }

    public void setFromLine(int fromLine) {
        this.fromLine = fromLine;
    }

    public File getCurrentFile() {
        return currentFile;
    }
}
