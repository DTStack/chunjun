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
import com.dtstack.flinkx.ftp.FtpHandler;
import com.dtstack.flinkx.ftp.SFtpHandler;
import com.dtstack.flinkx.ftp.StandardFtpHandler;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.reader.ByteRateLimiter;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

    protected Integer timeout = 60000;

    protected String connectMode = FtpConfigConstants.DEFAULT_FTP_CONNECT_PATTERN;

    protected String charsetName = "utf-8";

    protected List<Integer> columnIndex;

    protected List<String> columnValue;

    protected List<String> columnType;

    protected transient boolean isFirstLineHeader;

    private transient BufferedReader br;

    private transient FtpHandler ftpHandler;

    private transient String line;

    @Override
    public void configure(Configuration parameters) {
        if("sftp".equalsIgnoreCase(protocol)) {
            ftpHandler = new SFtpHandler();
        } else {
            ftpHandler = new StandardFtpHandler();
        }
        ftpHandler.loginFtpServer(host,username,password,port,timeout,connectMode);
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        List<String> files = ftpHandler.getFiles(path);
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
        FtpSeqInputStream is = new FtpSeqInputStream(ftpHandler, paths);

        br = new BufferedReader(new InputStreamReader(is, charsetName));

        if(StringUtils.isNotBlank(monitorUrls) && this.bytes > 0) {
            this.byteRateLimiter = new ByteRateLimiter(getRuntimeContext(), monitorUrls, bytes, 1);
            this.byteRateLimiter.start();
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        line = br.readLine();

        // if first line is header,then read next line
        if(isFirstLineHeader){
            line = br.readLine();
            isFirstLineHeader = false;
        }
        return line == null;
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        row = new Row(columnIndex.size());
        String[] fields = line.split(delimiter);
        for(int i = 0; i < columnIndex.size(); ++i) {
            Integer index = columnIndex.get(i);
            String val = columnValue.get(i);
            if(index != null) {
                String col = fields[index];
                row.setField(i, col);
            } else if(val != null) {
                String type = columnType.get(i);
                Object col = StringUtil.string2col(val,type);
                row.setField(i, col);
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
