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
import com.dtstack.chunjun.connector.ftp.client.ZipInputStream;
import com.dtstack.chunjun.connector.ftp.config.FtpConfig;
import com.dtstack.chunjun.connector.ftp.enums.FileType;
import com.dtstack.chunjun.connector.ftp.extend.ftp.File;
import com.dtstack.chunjun.connector.ftp.extend.ftp.FtpParseException;
import com.dtstack.chunjun.connector.ftp.extend.ftp.IFormatConfig;
import com.dtstack.chunjun.connector.ftp.extend.ftp.concurrent.FtpFileSplit;
import com.dtstack.chunjun.connector.ftp.extend.ftp.format.IFileReadFormat;
import com.dtstack.chunjun.connector.ftp.handler.DTFtpHandler;
import com.dtstack.chunjun.connector.ftp.handler.FtpHandler;
import com.dtstack.chunjun.connector.ftp.handler.FtpHandlerFactory;
import com.dtstack.chunjun.connector.ftp.handler.Position;
import com.dtstack.chunjun.connector.ftp.iformat.IFormatFactory;
import com.dtstack.chunjun.metrics.BaseMetric;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import static com.dtstack.chunjun.connector.ftp.config.ConfigConstants.FTP_COUNTER_PREFIX;

@Slf4j
public class FtpFileReader {

    private DTFtpHandler ftpHandler;
    private Iterator<FtpFileSplit> iter;
    private int fromLine = 0;

    /** the fileSplit current read * */
    private FtpFileSplit currentFileSplit;

    private Position startPosition;
    private final FtpConfig ftpConfig;
    private IFormatConfig iFormatConfig;
    private IFileReadFormat currentFileReadFormat;
    private final Map<FileType, IFileReadFormat> iFileReadFormatCache;
    private RuntimeContext runtimeContext;
    private Map<String, LongCounter> counterMap;
    private BaseMetric baseMetric;
    private boolean enableMetric = false;
    private FtpInputStreamProxy currentInputStream;

    public FtpFileReader(
            DTFtpHandler ftpHandler,
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
            FtpFileSplit storefs = startPosition.getFileSplit();

            /*
             * remove same file name but endPosition < startPosition.getCurrentReadPosition() set
             * FtpFileSplit startPosition = startPosition.getCurrentReadPosition()
             */
            ArrayList<FtpFileSplit> fileCache = new ArrayList<>();
            for (Iterator<FtpFileSplit> it = iter; it.hasNext(); ) {
                FtpFileSplit fs = it.next();
                if (fs.getFileAbsolutePath().equals(storefs.getFileAbsolutePath())) {
                    if (fs.getEndPosition() <= startPosition.getCurrentReadPosition()) {
                        // remove same file name but endPosition <
                        // startPosition.getCurrentReadPosition()
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
            if (!currentFileReadFormat.hasNext()) {
                close();
                return readLine();
            }

            try {
                String[] record = currentFileReadFormat.nextRecord();
                if (enableMetric) {
                    String readBytesMetricName = getMetricName("readBytes");
                    LongCounter readBytesCounter = counterMap.get(readBytesMetricName);
                    setMetricValue(readBytesCounter, currentInputStream.getCurrentReadBytes());

                    String metricName = getMetricName("readLines");
                    LongCounter counter = counterMap.get(metricName);
                    updateMetric(counter, 1);
                }

                return new Data(
                        record,
                        new Position(
                                currentInputStream.getCurrentReadBytes()
                                        + currentFileSplit.getStartPosition(),
                                currentFileSplit));

            } catch (FtpParseException e) {
                String[] record = new String[] {e.getContent()};
                return new Data(
                        record,
                        new Position(
                                currentInputStream.getCurrentReadBytes()
                                        + currentFileSplit.getStartPosition(),
                                currentFileSplit),
                        e);
            }

        } else {
            if (enableMetric) {
                //  tick end
                String metricName = getMetricName("tickEnd");
                LongCounter counter = counterMap.get(metricName);
                tickEndMetric(counter);
            }
            return null;
        }
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
            currentFileSplit = fileSplit;
            currentInputStream = new FtpInputStreamProxy(in, currentFileSplit.getReadLimit());

            // adapt to previous file parameter
            File file =
                    new File(
                            ftpConfig.getPath(),
                            fileSplit.getFileAbsolutePath(),
                            fileSplit.getFilename(),
                            fileSplit.getCompressType());
            currentFileReadFormat.open(file, currentInputStream, iFormatConfig);

            if (fileSplit.getStartPosition() == 0) {
                if (fileType != FileType.EXCEL) {
                    for (int i = 0; i < fromLine; i++) {
                        if (currentFileReadFormat.hasNext()) {
                            String[] strings = currentFileReadFormat.nextRecord();
                            log.info("Skip line:{}", Arrays.toString(strings));
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

        if (currentInputStream != null) {
            currentInputStream.close();
        }

        if (currentFileReadFormat != null) {
            currentFileReadFormat.close();
            currentFileReadFormat = null;

            if (ftpHandler instanceof FtpHandler) {
                try {
                    ((FtpHandler) ftpHandler).getFtpClient().completePendingCommand();
                } catch (Exception e) {
                    // 如果出现了超时异常，就直接获取一个新的ftpHandler
                    log.warn("FTPClient completePendingCommand has error ->", e);
                    try {
                        ftpHandler.logoutFtpServer();
                    } catch (Exception exception) {
                        log.warn("FTPClient logout has error ->", exception);
                    }
                    ftpHandler = FtpHandlerFactory.createFtpHandler(ftpConfig.getProtocol());
                    ftpHandler.loginFtpServer(ftpConfig);
                }
            }
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

        log.info("The file [{}]  extension is {}  ", fileName, fileType);

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

    public IFormatConfig getiFormatConfig() {
        return iFormatConfig;
    }

    public void setiFormatConfig(IFormatConfig iFormatConfig) {
        this.iFormatConfig = iFormatConfig;
    }

    public void enableMetric(RuntimeContext runtimeContext, BaseMetric baseMetric) {
        this.runtimeContext = runtimeContext;
        this.baseMetric = baseMetric;
        counterMap = new HashMap<>();
        this.enableMetric = true;

        ArrayList<FtpFileSplit> fileCache = new ArrayList<>();

        int totalFiles = 0;
        StringBuilder allFileNamesBuilder = new StringBuilder();
        allFileNamesBuilder.append("ftp_read_files(");

        for (Iterator<FtpFileSplit> it = iter; it.hasNext(); ) {
            FtpFileSplit file = it.next();
            String readLinesMetricName = getMetricName("readLines");
            String readBytesMetricName = getMetricName("readBytes");

            counterMap.put(readLinesMetricName, registerMetric(readLinesMetricName, true));
            counterMap.put(readBytesMetricName, registerMetric(readBytesMetricName, false));

            totalFiles += 1;
            allFileNamesBuilder.append(file.getFileAbsolutePath());

            if (it.hasNext()) {
                allFileNamesBuilder.append(", ");
            }
            fileCache.add(file);
        }

        allFileNamesBuilder.append(')');
        String totalFileMetricName = allFileNamesBuilder.toString();
        LongCounter totalFileCounter = registerMetric(totalFileMetricName, false);
        counterMap.put(totalFileMetricName, registerMetric(totalFileMetricName, false));
        updateMetric(totalFileCounter, totalFiles);

        // tick start
        String tickStartMetric = getMetricName("tickStart");
        LongCounter durationCounter = registerMetric(tickStartMetric, false);
        counterMap.put(tickStartMetric, durationCounter);
        tickStartMetric(durationCounter);

        iter = fileCache.iterator();
    }

    private String getMetricName(String action) {
        switch (action) {
            case "tickStart":
            case "tickEnd":
                return FTP_COUNTER_PREFIX + "_read_duration";

            case "readLines":
                return FTP_COUNTER_PREFIX + "_read_lines";

            case "readBytes":
                return FTP_COUNTER_PREFIX + "_read_bytes";

            default:
                throw new RuntimeException("illegal parameter");
        }
    }

    public LongCounter registerMetric(String metricName, boolean meterView) {
        LongCounter counter = runtimeContext.getLongCounter(metricName);
        baseMetric.addMetric(metricName, counter, meterView);
        return counter;
    }

    public void updateMetric(LongCounter counter, long incr) {
        counter.add(incr);
    }

    public void setMetricValue(LongCounter counter, Long value) {
        counter.resetLocal();
        counter.add(value);
    }

    public void tickStartMetric(LongCounter counter) {
        if (counter != null) {
            counter.resetLocal();
            counter.add(System.currentTimeMillis());
        }
    }

    public void tickEndMetric(LongCounter counter) {
        if (counter != null) {
            Long startTime = counter.getLocalValue();
            counter.resetLocal();
            counter.add(System.currentTimeMillis() - startTime);
        }
    }
}
