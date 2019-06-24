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

import com.dtstack.flinkx.ftp.FtpConfigConstants;
import com.dtstack.flinkx.ftp.IFtpHandler;
import com.dtstack.flinkx.ftp.SFtpHandler;
import com.dtstack.flinkx.ftp.FtpHandler;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
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
public class FtpInputFormat extends RichInputFormat {

    protected String path;

    protected String host;

    protected Integer port;

    protected String username;

    protected String password;

    protected String delimiter = ",";

    protected String protocol;

    protected Integer timeout;

    protected String connectMode = FtpConfigConstants.DEFAULT_FTP_CONNECT_PATTERN;

    protected String charsetName = "utf-8";

    protected List<MetaColumn> metaColumns;

    protected boolean isFirstLineHeader;

    private transient FtpSeqBufferedReader br;

    private transient IFtpHandler ftpHandler;

    private transient String line;

    @Override
    public void configure(Configuration parameters) {
        if("sftp".equalsIgnoreCase(protocol)) {
            ftpHandler = new SFtpHandler();
        } else {
            ftpHandler = new FtpHandler();
        }
        ftpHandler.loginFtpServer(host,username,password,port,timeout,connectMode);
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        List<String> files = new ArrayList<>();

        if(path != null && path.length() > 0){
            path = path.replace("\n","").replace("\r","");
            String[] pathArray = path.split(",");
            for (String p : pathArray) {
                files.addAll(ftpHandler.getFiles(p.trim()));
            }
        }

        int numSplits = (files.size() < minNumSplits ?  files.size() : minNumSplits);
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
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void openInternal(InputSplit split) throws IOException {
        FtpInputSplit inputSplit = (FtpInputSplit)split;
        List<String> paths = inputSplit.getPaths();

        if (isFirstLineHeader){
            br = new FtpSeqBufferedReader(ftpHandler,paths.iterator());
            br.setFromLine(1);
        } else {
            br = new FtpSeqBufferedReader(ftpHandler,paths.iterator());
            br.setFromLine(0);
        }
        br.setCharsetName(charsetName);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        line = br.readLine();
        return line == null;
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        String[] fields = line.split(delimiter);
        if (metaColumns.size() == 1 && "*".equals(metaColumns.get(0).getName())){
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
