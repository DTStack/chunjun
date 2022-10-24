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

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.ftp.client.Data;
import com.dtstack.chunjun.connector.ftp.client.File;
import com.dtstack.chunjun.connector.ftp.client.FileType;
import com.dtstack.chunjun.connector.ftp.client.FileUtil;
import com.dtstack.chunjun.connector.ftp.conf.FtpConfig;
import com.dtstack.chunjun.connector.ftp.converter.FtpColumnConverter;
import com.dtstack.chunjun.connector.ftp.converter.FtpRowConverter;
import com.dtstack.chunjun.connector.ftp.handler.FtpHandlerFactory;
import com.dtstack.chunjun.connector.ftp.handler.IFtpHandler;
import com.dtstack.chunjun.connector.ftp.handler.Position;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * The InputFormat class of Ftp
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class FtpInputFormat extends BaseRichInputFormat {

    protected FtpConfig ftpConfig;

    private transient FtpSeqBufferedReader br;

    private transient IFtpHandler ftpHandler;

    private transient Data data;

    private transient Position position;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        ftpHandler = FtpHandlerFactory.createFtpHandler(ftpConfig.getProtocol());
        ftpHandler.loginFtpServer(ftpConfig);
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        try (IFtpHandler ftpHandler = FtpHandlerFactory.createFtpHandler(ftpConfig.getProtocol())) {
            ftpHandler.loginFtpServer(ftpConfig);

            List<String> files = new ArrayList<>();

            String path = ftpConfig.getPath();
            if (path != null && path.length() > 0) {
                path = path.replace("\n", "").replace("\r", "");
                String[] pathArray = path.split(",");
                for (String p : pathArray) {
                    files.addAll(ftpHandler.getFiles(p.trim()));
                }
            }

            List<File> fileList = new ArrayList<>();

            if (CollectionUtils.isNotEmpty(files)) {
                for (String filePath : files) {
                    if (StringUtils.isNotBlank(ftpConfig.getCompressType())) {
                        FileUtil.addFile(ftpHandler, filePath, ftpConfig, fileList);
                    } else {
                        fileList.add(
                                new File(
                                        null,
                                        filePath,
                                        filePath.substring(filePath.lastIndexOf("/") + 1),
                                        null));
                    }
                }
            }
            if (CollectionUtils.isEmpty(fileList)) {
                throw new RuntimeException("There are no readable files  in directory " + path);
            }
            LOG.info("FTP files = {}", GsonUtil.GSON.toJson(fileList));
            int numSplits = (Math.min(files.size(), minNumSplits));
            FtpInputSplit[] ftpInputSplits = new FtpInputSplit[numSplits];
            for (int index = 0; index < numSplits; ++index) {
                ftpInputSplits[index] = new FtpInputSplit();
            }

            fileList.sort(Comparator.comparing(File::getFileAbsolutePath));

            for (int i = 0; i < fileList.size(); ++i) {
                ftpInputSplits[i % numSplits].getPaths().add(fileList.get(i));
            }
            return ftpInputSplits;
        }
    }

    @Override
    public void openInternal(InputSplit split) throws IOException {
        FtpInputSplit inputSplit = (FtpInputSplit) split;
        List<File> paths = inputSplit.getPaths();
        removeFileHasRead(paths);
        LOG.info("read files = {}", GsonUtil.GSON.toJson(paths));
        Position position =
                (formatState != null && formatState.getState() != null)
                        ? (Position) formatState.getState()
                        : null;

        if (ftpConfig.getIsFirstLineHeader()) {
            br = new FtpSeqBufferedReader(ftpHandler, paths.iterator(), ftpConfig, position);
            if (FileType.fromString(ftpConfig.getFileType()) != FileType.EXCEL) {
                br.setFromLine(1);
            }
        } else {
            br = new FtpSeqBufferedReader(ftpHandler, paths.iterator(), ftpConfig, position);
            br.setFromLine(0);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        data = br.readLine();
        return data == null || data.getData() == null;
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        String[] fields = data.getData();
        try {
            if (fields.length == 1 && StringUtils.isBlank(fields[0])) {
                LOG.warn("read data:{}, it will not be written.", Arrays.toString(fields));
                return null;
            }

            if (rowConverter instanceof FtpRowConverter) {
                rowData = rowConverter.toInternal(String.join(",", fields));
            } else if (rowConverter instanceof FtpColumnConverter) {

                List<FieldConf> columns = ftpConfig.getColumn();

                GenericRowData genericRowData;
                if (CollectionUtils.size(columns) == 1
                        && ConstantValue.STAR_SYMBOL.equals(columns.get(0).getName())) {
                    genericRowData = new GenericRowData(fields.length);
                    for (int i = 0; i < fields.length; i++) {
                        Object value = fields[i];
                        if ("".equals(value) && null != ftpConfig.getNullIsReplacedWithValue()) {
                            value = ftpConfig.getNullIsReplacedWithValue();
                        }
                        genericRowData.setField(i, value);
                    }
                } else {
                    genericRowData = new GenericRowData(columns.size());
                    for (int i = 0; i < CollectionUtils.size(columns); i++) {
                        FieldConf fieldConf = columns.get(i);

                        Object value = null;
                        if (fieldConf.getValue() != null) {
                            value = fieldConf.getValue();
                        } else if (fieldConf.getIndex() != null
                                && fieldConf.getIndex() < fields.length) {
                            value = fields[fieldConf.getIndex()];
                        }
                        if ("".equals(value) && null != ftpConfig.getNullIsReplacedWithValue()) {
                            value = ftpConfig.getNullIsReplacedWithValue();
                        }
                        genericRowData.setField(i, value);
                    }
                }
                rowData = rowConverter.toInternal(genericRowData);
            }
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
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
        if (br != null) {
            br.close();
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

    /** 移除已经读取的文件* */
    public void removeFileHasRead(List<File> files) {
        if (formatState != null && formatState.getState() != null) {
            LOG.info("start remove the file according to the state value...");
            Position state = (Position) formatState.getState();
            Iterator<File> iterator = files.iterator();
            while (iterator.hasNext()) {
                File next = iterator.next();
                if (!state.getFile().getFileAbsolutePath().equals(next.getFileAbsolutePath())) {
                    LOG.info("skip file {} when recovery from state", next.getFileAbsolutePath());
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
    }
}
