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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.ftp.client.Data;
import com.dtstack.chunjun.connector.ftp.config.ConfigConstants;
import com.dtstack.chunjun.connector.ftp.config.FtpConfig;
import com.dtstack.chunjun.connector.ftp.converter.FtpSqlConverter;
import com.dtstack.chunjun.connector.ftp.converter.FtpSyncConverter;
import com.dtstack.chunjun.connector.ftp.extend.ftp.IFormatConfig;
import com.dtstack.chunjun.connector.ftp.extend.ftp.concurrent.ConcurrentFileSplit;
import com.dtstack.chunjun.connector.ftp.extend.ftp.concurrent.FtpFileSplit;
import com.dtstack.chunjun.connector.ftp.handler.DTFtpHandler;
import com.dtstack.chunjun.connector.ftp.handler.FtpHandlerFactory;
import com.dtstack.chunjun.connector.ftp.handler.Position;
import com.dtstack.chunjun.connector.ftp.spliter.ConcurrentFileSplitFactory;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.PrintUtil;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.dtstack.chunjun.connector.ftp.config.ConfigConstants.FTP_COUNTER_PREFIX;

@Slf4j
public class FtpInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = 6065928763165195830L;

    public static char[] REGEX_CHARS =
            new char[] {'*', '?', '+', '|', '(', ')', '{', '}', '[', ']', '\\', '$', '^'};

    protected FtpConfig ftpConfig;

    private transient FtpFileReader reader;

    private transient DTFtpHandler ftpHandler;

    private transient Data data;

    private transient Position position;

    private boolean enableFilenameRow;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        enableFilenameRow =
                ftpConfig.getColumn().stream()
                        .anyMatch(
                                column ->
                                        column.getName().equals(ConfigConstants.INTERNAL_FILENAME));
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        DTFtpHandler ftpHandler = FtpHandlerFactory.createFtpHandler(ftpConfig.getProtocol());
        ftpHandler.loginFtpServer(ftpConfig);

        List<String> files = new ArrayList<>();

        try {
            String path = ftpConfig.getPath();
            if (path != null && path.length() > 0) {
                path = path.replace("\n", "").replace("\r", "");
                String[] pathArray = path.split(",");
                for (String p : pathArray) {
                    files.addAll(listFilesInPath(ftpHandler, p));
                }
            }

            ConcurrentFileSplit splitter =
                    ConcurrentFileSplitFactory.createConcurrentFileSplit(ftpConfig);
            List<FtpFileSplit> fileList =
                    splitter.buildFtpFileSplit(ftpHandler, buildIFormatConfig(ftpConfig), files);

            FtpInputSplit[] ftpInputSplits = new FtpInputSplit[minNumSplits];
            for (int index = 0; index < minNumSplits; ++index) {
                ftpInputSplits[index] = new FtpInputSplit();
            }

            for (int i = 0; i < fileList.size(); ++i) {
                ftpInputSplits[i % minNumSplits].getFileSplits().add(fileList.get(i));
            }
            return ftpInputSplits;
        } finally {
            ftpHandler.logoutFtpServer();
        }
    }

    @Override
    public void openInternal(InputSplit split) throws IOException {
        ftpHandler = FtpHandlerFactory.createFtpHandler(ftpConfig.getProtocol());
        ftpHandler.loginFtpServer(ftpConfig);

        FtpInputSplit inputSplit = (FtpInputSplit) split;
        List<FtpFileSplit> fileSplits = inputSplit.getFileSplits();
        removeFileHasRead(fileSplits);

        Position position =
                (formatState != null && formatState.getState() != null)
                        ? (Position) formatState.getState()
                        : null;

        if (ftpConfig.isFirstLineHeader()) {
            reader = new FtpFileReader(ftpHandler, fileSplits.iterator(), ftpConfig, position);
            reader.setFromLine(1);
        } else {
            reader = new FtpFileReader(ftpHandler, fileSplits.iterator(), ftpConfig, position);
            reader.setFromLine(0);
        }

        reader.setiFormatConfig(buildIFormatConfig(ftpConfig));
        reader.enableMetric(getRuntimeContext(), inputMetric);
        reader.skipHasReadFiles();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        data = reader.readLine();
        return data == null || data.getData() == null;
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        String[] fields = data.getData();
        if (data.getException() != null) {
            throw new ReadRecordException(
                    data.getException().getMessage(), data.getException(), 0, rowData);
        }

        try {
            if (fields.length == 1 && StringUtils.isBlank(fields[0])) {
                log.warn("read data:{}, it will not be written.", Arrays.toString(fields));
                return null;
            }

            if (rowConverter instanceof FtpSqlConverter) {
                // 处理字段配置了对应的列索引
                if (ftpConfig.getColumnIndex() != null) {
                    List<FieldConfig> columns = ftpConfig.getColumn();
                    String[] fieldsData = new String[columns.size()];
                    for (int i = 0; i < CollectionUtils.size(columns); i++) {
                        FieldConfig fieldConfig = columns.get(i);
                        if (fieldConfig.getIndex() >= fields.length) {
                            String errorMessage =
                                    String.format(
                                            "The column index is greater than the data size."
                                                    + " The current column index is [%s], but the data size is [%s]. Data loss may occur.",
                                            fieldConfig.getIndex(), fields.length);
                            throw new IllegalArgumentException(errorMessage);
                        }
                        fieldsData[i] = fields[fieldConfig.getIndex()];
                    }
                    fields = fieldsData;
                }
                // 解决数据里包含特殊符号(逗号、换行符)
                rowData = rowConverter.toInternal(fields);
            } else if (rowConverter instanceof FtpSyncConverter) {

                List<FieldConfig> columns = ftpConfig.getColumn();

                if (enableFilenameRow) {
                    List<FieldConfig> tmpColumns = ftpConfig.getColumn();
                    int tmpIndex = 0;
                    for (int i = 0; i < tmpColumns.size(); i++) {
                        if (tmpColumns.get(i).getName().equals(ConfigConstants.INTERNAL_FILENAME)) {
                            tmpIndex = i;
                            break;
                        }
                    }

                    FieldConfig tmpColumn = columns.get(tmpIndex);
                    tmpColumn.setValue(reader.getCurrentFileName());
                    columns.set(tmpIndex, tmpColumn);
                }
                GenericRowData genericRowData;
                if (CollectionUtils.size(columns) == 1
                        && ConstantValue.STAR_SYMBOL.equals(columns.get(0).getName())) {
                    genericRowData = new GenericRowData(fields.length);
                    for (int i = 0; i < fields.length; i++) {
                        Object value = fields[i];
                        if (null == value || "".equals(value)) {
                            value = ftpConfig.getNullIsReplacedWithValue();
                        }
                        genericRowData.setField(i, value);
                    }
                } else {
                    genericRowData = new GenericRowData(columns.size());
                    for (int i = 0; i < CollectionUtils.size(columns); i++) {
                        FieldConfig fieldConfig = columns.get(i);

                        Object value;
                        if (fieldConfig.getValue() != null) {
                            value = fieldConfig.getValue();
                        } else {
                            if (fieldConfig.getIndex() >= fields.length) {
                                String errorMessage =
                                        String.format(
                                                "The column index is greater than the data size."
                                                        + " The current column index is [%s], but the data size is [%s]. Data loss may occur.",
                                                fieldConfig.getIndex(), fields.length);
                                throw new IllegalArgumentException(errorMessage);
                            }
                            value = fields[fieldConfig.getIndex()];
                        }
                        if (null == value || "".equals(value)) {
                            value = ftpConfig.getNullIsReplacedWithValue();
                        }
                        genericRowData.setField(i, value);
                    }
                }
                rowData = rowConverter.toInternal(genericRowData);
            }
        } catch (Exception e) {
            throw new ReadRecordException("Read data error.", e, 0, rowData);
        }
        position = data.getPosition();
        return rowData;
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        if (formatState != null) {
            formatState.setState(position);
        }
        return formatState;
    }

    @Override
    public void closeInternal() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (ftpHandler != null) {
            ftpHandler.logoutFtpServer();
        }
    }

    public FtpConfig getFtpConfig() {
        return ftpConfig;
    }

    public void setFtpConfig(FtpConfig ftpConfig) {
        this.ftpConfig = ftpConfig;
    }

    private IFormatConfig buildIFormatConfig(FtpConfig ftpConfig) {
        IFormatConfig iFormatConfig = new IFormatConfig();
        iFormatConfig.setFirstLineHeader(ftpConfig.isFirstLineHeader());
        iFormatConfig.setEncoding(ftpConfig.getEncoding());
        iFormatConfig.setFieldDelimiter(ftpConfig.getFieldDelimiter());
        iFormatConfig.setFileConfig(ftpConfig.getFileConfig());
        final String[] fields = new String[ftpConfig.getColumn().size()];
        IntStream.range(0, fields.length)
                .forEach(i -> fields[i] = ftpConfig.getColumn().get(i).getName());
        iFormatConfig.setFields(fields);
        iFormatConfig.setFetchMaxSize(ftpConfig.getMaxFetchSize());
        iFormatConfig.setParallelism(ftpConfig.getParallelism());
        iFormatConfig.setColumnDelimiter(ftpConfig.getColumnDelimiter());
        iFormatConfig.setSheetNo(ftpConfig.getSheetNo());

        return iFormatConfig;
    }

    private List<String> listFilesInPath(DTFtpHandler ftpHandler, String path) {
        path = path.trim();
        String fileRegex = path.substring(path.lastIndexOf("/") + 1);
        boolean isRegex = StringUtils.containsAny(fileRegex, REGEX_CHARS);
        if (isRegex) {
            String pathWithoutRegex = path.substring(0, path.lastIndexOf("/"));
            List<String> files = ftpHandler.getFiles(pathWithoutRegex);

            Pattern pattern = Pattern.compile(fileRegex);
            files.removeIf(
                    s -> {
                        String fileName = s.substring(s.lastIndexOf("/") + 1);
                        return !pattern.matcher(fileName).matches();
                    });

            return files;
        } else {
            return ftpHandler.getFiles(path);
        }
    }

    /** 移除已经读取的文件* */
    private void removeFileHasRead(List<FtpFileSplit> fileSplits) {
        if (formatState != null && formatState.getState() != null) {
            log.info("start remove the file according to the state value...");
            Position state = (Position) formatState.getState();
            Iterator<FtpFileSplit> iterator = fileSplits.iterator();
            while (iterator.hasNext()) {
                FtpFileSplit next = iterator.next();
                if (!state.getFileSplit()
                        .getFileAbsolutePath()
                        .equals(next.getFileAbsolutePath())) {
                    log.info("skip file {} when recovery from state", next.getFileAbsolutePath());
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public void closeInputFormat() {
        if (isClosed.get()) {
            return;
        }

        if (inputMetric == null) {
            return;
        }

        Map<String, LongCounter> allCounters = inputMetric.getMetricCounters();
        Map<String, Object> ftpCounter = new HashMap<>();

        allCounters.forEach(
                (key, value) -> {
                    if (key.startsWith(FTP_COUNTER_PREFIX)) {
                        ftpCounter.put(key, value.getLocalValue());
                    }
                });

        PrintUtil.printResult(ftpCounter);

        super.closeInputFormat();
    }
}
