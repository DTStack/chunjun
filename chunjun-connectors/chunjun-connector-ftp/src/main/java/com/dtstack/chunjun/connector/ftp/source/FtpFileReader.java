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

package com.dtstack.chunjun.connector.ftp.source;

import com.dtstack.chunjun.connector.ftp.client.Data;
import com.dtstack.chunjun.connector.ftp.client.File;
import com.dtstack.chunjun.connector.ftp.client.FileUtil;
import com.dtstack.chunjun.connector.ftp.client.ZipInputStream;
import com.dtstack.chunjun.connector.ftp.conf.FtpConfig;
import com.dtstack.chunjun.connector.ftp.enums.FileType;
import com.dtstack.chunjun.connector.ftp.format.IFileReadFormat;
import com.dtstack.chunjun.connector.ftp.format.IFormatConfig;
import com.dtstack.chunjun.connector.ftp.format.IFormatFactory;
import com.dtstack.chunjun.connector.ftp.handler.IFtpHandler;
import com.dtstack.chunjun.connector.ftp.handler.Position;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

public class FtpFileReader {

    private static final Logger LOG = LoggerFactory.getLogger(FtpFileReader.class);
    private final IFtpHandler ftpHandler;
    private Iterator<FtpFileSplit> iter;
    private int fromLine = 0;

    /** the fileSplit current read * */
    private FtpFileSplit currentFileSplit;

    /** The bytes read from the current fileSplit * */
    private Long currentFileSplitReadBytes = 0L;

    private Position startPosition;
    private final FtpConfig ftpConfig;
    private IFormatConfig iFormatConfig;
    private IFileReadFormat currentFileReadFormat;
    private final Map<FileType, IFileReadFormat> iFileReadFormatCache;

    public FtpFileReader(
            IFtpHandler ftpHandler,
            Iterator<FtpFileSplit> iter,
            FtpConfig ftpConfig,
            Position startPosition) {
        this.ftpHandler = ftpHandler;
        this.iter = iter;
        this.ftpConfig = ftpConfig;
        this.startPosition = startPosition;
        this.iFileReadFormatCache = new HashMap<>();
    }

    /** 断点续跑，过滤文件已经读过的部分 * */
    public void skipHasReadFiles() {
        if (startPosition != null && startPosition.getCurrentReadPosition() > 0) {
            FtpFileSplit storeFileSplit = startPosition.getFileSplit();

            /*
             * remove same file name but endPosition < startPosition.getCurrentReadPosition() set
             * FtpFileSplit startPosition = startPosition.getCurrentReadPosition()
             */
            ArrayList<FtpFileSplit> fileCache = new ArrayList<>();
            for (Iterator<FtpFileSplit> it = iter; it.hasNext(); ) {
                FtpFileSplit fs = it.next();
                if (fs.getFileAbsolutePath().equals(storeFileSplit.getFileAbsolutePath())) {
                    if (fs.getEndPosition() <= startPosition.getCurrentReadPosition()) {
                        // remove same file name but endPosition <
                        // startPosition.getCurrentReadPosition()
                        // do nothing
                    } else {
                        fs.setStartPosition(startPosition.getCurrentReadPosition());
                        fileCache.add(fs);
                    }
                } else {
                    fileCache.add(fs);
                }
            }
            startPosition = null;
            iter = fileCache.iterator();
        }
    }

    public Data readLine() throws IOException {
        if (currentFileReadFormat == null) {
            nextStream();
        }

        if (currentFileReadFormat != null) {
            if (currentFileSplitReadBytes >= currentFileSplit.getReadLimit()) {
                close();
                return readLine();
            }

            if (!currentFileReadFormat.hasNext()) {
                close();
                return readLine();
            }

            String[] record = currentFileReadFormat.nextRecord();
            addCurrentReadSize(record);

            return new Data(record, new Position(currentFileSplitReadBytes, currentFileSplit));
        }

        return null;
    }

    private void nextStream() throws IOException {
        if (iter.hasNext()) {
            FtpFileSplit fileSplit = iter.next();
            InputStream in;

            if (fileSplit.getCompressType() != null) {
                // If it is a compressed file, InputStream needs to be converted.
                in = getCompressStream(fileSplit);
            } else {
                in =
                        ftpHandler.getInputStreamByPosition(
                                fileSplit.getFileAbsolutePath(), fileSplit.getStartPosition());
            }

            if (in == null) {
                throw new RuntimeException(
                        String.format(
                                "can not get inputStream for file [%s], please check file read and write permissions",
                                fileSplit));
            }

            FileType fileType =
                    getFileType(
                            fileSplit.getFileAbsolutePath(),
                            ftpConfig.getFileType(),
                            StringUtils.isNotEmpty(ftpConfig.getCustomFormatClassName()));

            if (!iFileReadFormatCache.containsKey(fileType)) {
                IFileReadFormat iFileReadFormat =
                        IFormatFactory.create(fileType, ftpConfig.getCustomFormatClassName());
                iFileReadFormatCache.put(fileType, iFileReadFormat);
            }
            currentFileReadFormat = iFileReadFormatCache.get(fileType);

            // adapt to previous file parameter
            File file =
                    new File(
                            ftpConfig.getPath(),
                            fileSplit.getFileAbsolutePath(),
                            fileSplit.getFilename(),
                            fileSplit.getCompressType());
            currentFileReadFormat.open(file, in, iFormatConfig);

            currentFileSplit = fileSplit;
            currentFileSplitReadBytes = 0L;

            if (fileSplit.getStartPosition() == 0) {
                if (fileType != FileType.EXCEL) {
                    for (int i = 0; i < fromLine; i++) {
                        if (currentFileReadFormat.hasNext()) {
                            String[] strings = currentFileReadFormat.nextRecord();
                            LOG.info("Skip line:{}", Arrays.toString(strings));
                            addCurrentReadSize(strings);
                        } else {
                            break;
                        }
                    }
                }
            }
        } else {
            currentFileReadFormat = null;
        }
    }

    public void close() throws IOException {
        if (currentFileReadFormat != null) {
            currentFileReadFormat.close();
            currentFileReadFormat = null;

            FileUtil.closeWithFtpHandler(ftpHandler, LOG);
        }
    }

    private FileType getFileType(String fileName, String defaultType, boolean customFormat) {

        if (customFormat) {
            return FileType.CUSTOM;
        }

        String fileType = "";
        if (StringUtils.isNotBlank(defaultType)) {
            fileType = defaultType;
        } else {
            int i = fileName.lastIndexOf(".");
            if (i != -1 && i != fileName.length() - 1) {
                fileType = fileName.substring(i + 1);
            }
        }

        LOG.info("The file [{}]  extension is {}  ", fileName, fileType);

        return FileType.getType(fileType);
    }

    private InputStream getCompressStream(FtpFileSplit fileSplit) {
        if ("ZIP".equals(fileSplit.getCompressType().toUpperCase(Locale.ENGLISH))) {
            InputStream inputStream = ftpHandler.getInputStream(fileSplit.getFileAbsolutePath());
            ZipInputStream zipInputStream = new ZipInputStream(inputStream);
            zipInputStream.addFileName(fileSplit.getFilename());
            return zipInputStream;
        }
        throw new RuntimeException("Support zip file, not support " + fileSplit.getCompressType());
    }

    public void setFromLine(int fromLine) {
        this.fromLine = fromLine;
    }

    public String getCurrentFileName() {
        return currentFileSplit.getFileAbsolutePath();
    }

    public void setIFormatConfig(IFormatConfig iFormatConfig) {
        this.iFormatConfig = iFormatConfig;
    }

    private void addCurrentReadSize(String[] value) {
        String line = String.join(ftpConfig.getFieldDelimiter(), value);
        currentFileSplitReadBytes += line.getBytes(getCharacterSet()).length;
        currentFileSplitReadBytes += "\n".getBytes(getCharacterSet()).length;
    }

    private Charset getCharacterSet() {
        switch (ftpConfig.encoding) {
            case "gbk":
                return Charset.forName("GBK");
            case "utf-8":
            default:
                return StandardCharsets.UTF_8;
        }
    }
}
