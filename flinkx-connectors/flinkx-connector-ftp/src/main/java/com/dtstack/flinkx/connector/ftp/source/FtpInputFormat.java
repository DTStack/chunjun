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

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.ftp.conf.FtpConfig;
import com.dtstack.flinkx.connector.ftp.converter.FtpColumnConverter;
import com.dtstack.flinkx.connector.ftp.converter.FtpRowConverter;
import com.dtstack.flinkx.connector.ftp.handler.FtpHandlerFactory;
import com.dtstack.flinkx.connector.ftp.handler.IFtpHandler;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.source.format.BaseRichInputFormat;
import com.dtstack.flinkx.throwable.ReadRecordException;
import com.dtstack.flinkx.util.GsonUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
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

    private transient String line;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        ftpHandler = FtpHandlerFactory.createFtpHandler(ftpConfig.getProtocol());
        ftpHandler.loginFtpServer(ftpConfig);
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        IFtpHandler ftpHandler = FtpHandlerFactory.createFtpHandler(ftpConfig.getProtocol());
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
        LOG.info("FTP files = {}", GsonUtil.GSON.toJson(files));
        int numSplits = (Math.min(files.size(), minNumSplits));
        FtpInputSplit[] ftpInputSplits = new FtpInputSplit[numSplits];
        for (int index = 0; index < numSplits; ++index) {
            ftpInputSplits[index] = new FtpInputSplit();
        }
        for (int i = 0; i < files.size(); ++i) {
            ftpInputSplits[i % numSplits].getPaths().add(files.get(i));
        }

        ftpHandler.logoutFtpServer();
        return ftpInputSplits;
    }

    @Override
    public void openInternal(InputSplit split) throws IOException {
        FtpInputSplit inputSplit = (FtpInputSplit) split;
        List<String> paths = inputSplit.getPaths();

        if (ftpConfig.getIsFirstLineHeader()) {
            br = new FtpSeqBufferedReader(ftpHandler, paths.iterator(), ftpConfig);
            br.setFromLine(1);
        } else {
            br = new FtpSeqBufferedReader(ftpHandler, paths.iterator(), ftpConfig);
            br.setFromLine(0);
        }
        br.setFileEncoding(ftpConfig.getEncoding());
    }

    @Override
    public boolean reachedEnd() throws IOException {
        line = br.readLine();
        return line == null;
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {

        try {
            if (StringUtils.isBlank(line)) {
                LOG.warn("read data:{}, it will not be written.", line);
                return null;
            }

            if (rowConverter instanceof FtpRowConverter) {
                rowData = rowConverter.toInternal(line);
            } else if (rowConverter instanceof FtpColumnConverter) {
                String[] fields = line.split(ftpConfig.getFieldDelimiter());
                List<FieldConf> columns = ftpConfig.getColumn();

                GenericRowData genericRowData;
                if (CollectionUtils.size(columns) == 1
                        && ConstantValue.STAR_SYMBOL.equals(columns.get(0).getName())) {
                    genericRowData = new GenericRowData(fields.length);
                    for (int i = 0; i < fields.length; i++) {
                        genericRowData.setField(i, fields[i]);
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
                        genericRowData.setField(i, value);
                    }
                }
                rowData = rowConverter.toInternal(genericRowData);
            }
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
        return rowData;
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
}
