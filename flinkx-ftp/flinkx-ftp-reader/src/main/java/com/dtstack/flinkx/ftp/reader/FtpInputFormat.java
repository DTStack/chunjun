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

package com.dtstack.flinkx.ftp.reader;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.ftp.FtpConfig;
import com.dtstack.flinkx.ftp.FtpHandlerFactory;
import com.dtstack.flinkx.ftp.IFtpHandler;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The InputFormat class of Ftp
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpInputFormat extends BaseRichInputFormat {

    protected FtpConfig ftpConfig;

    protected List<MetaColumn> metaColumns;

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
        if(path != null && path.length() > 0){
            path = path.replace("\n","").replace("\r","");
            String[] pathArray = path.split(",");
            for (String p : pathArray) {
                files.addAll(ftpHandler.getFiles(p.trim()));
            }
        }
        if (CollectionUtils.isEmpty(files)) {
            throw new RuntimeException("There are no readable files  in directory " + path);
        }
        LOG.info("FTP files = {}", GsonUtil.GSON.toJson(files));
        int numSplits = (Math.min(files.size(), minNumSplits));
        FtpInputSplit[] ftpInputSplits = new FtpInputSplit[numSplits];
        for(int index = 0; index < numSplits; ++index) {
            ftpInputSplits[index] = new FtpInputSplit();
        }
        for(int i = 0; i < files.size(); ++i) {
            ftpInputSplits[i % numSplits].getPaths().add(files.get(i));
        }

        ftpHandler.logoutFtpServer();
        return ftpInputSplits;
    }

    @Override
    public void openInternal(InputSplit split) throws IOException {
        FtpInputSplit inputSplit = (FtpInputSplit)split;
        List<String> paths = inputSplit.getPaths();

        if (ftpConfig.getIsFirstLineHeader()){
            br = new FtpSeqBufferedReader(ftpHandler,paths.iterator(),ftpConfig);
            br.setFromLine(1);
        } else {
            br = new FtpSeqBufferedReader(ftpHandler,paths.iterator(),ftpConfig);
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
    public Row nextRecordInternal(Row row) throws IOException {
        String[] fields = line.split(ftpConfig.getFieldDelimiter());
        if (metaColumns.size() == 1 && ConstantValue.STAR_SYMBOL.equals(metaColumns.get(0).getName())){
            row = new Row(fields.length);
            for (int i = 0; i < fields.length; i++) {
                row.setField(i, fields[i]);
            }
        } else {
            row = new Row(metaColumns.size());
            for (int i = 0; i < metaColumns.size(); i++) {
                MetaColumn metaColumn = metaColumns.get(i);

                Object value = null;
                if(metaColumn.getIndex() != null && metaColumn.getIndex() < fields.length){
                    value = fields[metaColumn.getIndex()];
                    if(((String) value).length() == 0){
                        value = metaColumn.getValue();
                    }
                } else if(metaColumn.getValue() != null){
                    value = metaColumn.getValue();
                }

                if(value != null){
                    value = StringUtil.string2col(String.valueOf(value),metaColumn.getType(),metaColumn.getTimeFormat());
                }

                row.setField(i, value);
            }
        }

        return row;
    }

    @Override
    public void closeInternal() throws IOException {
        if(br != null) {
            br.close();
        }
        if(ftpHandler != null) {
            ftpHandler.logoutFtpServer();
        }
    }
}
