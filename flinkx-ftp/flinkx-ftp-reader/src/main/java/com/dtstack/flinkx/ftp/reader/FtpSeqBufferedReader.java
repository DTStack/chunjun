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

import com.dtstack.flinkx.ftp.FtpConfig;
import com.dtstack.flinkx.ftp.FtpHandlerFactory;
import com.dtstack.flinkx.ftp.IFtpHandler;
import com.dtstack.flinkx.ftp.FtpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

    private Iterator<String> iter;

    private int fromLine = 0;

    private BufferedReader br;

    private String fileEncoding;

    //ftp配置信息
    private FtpConfig ftpConfig;

    public FtpSeqBufferedReader(IFtpHandler ftpHandler, Iterator<String> iter, FtpConfig ftpConfig) {
        this.ftpHandler = ftpHandler;
        this.iter = iter;
        this.ftpConfig = ftpConfig;
    }

    public String readLine() throws IOException{
        if (br == null){
            nextStream();
        }

        if(br != null){
            String line = br.readLine();
            if (line == null){
                close();
                return readLine();
            }

            return line;
        } else {
            return null;
        }
    }

    private void nextStream() throws IOException{
        if(iter.hasNext()){
            String file = iter.next();
            InputStream in = ftpHandler.getInputStream(file);
            if (in == null) {
                throw new RuntimeException(String.format("can not get inputStream for file [%s], please check file read and write permissions", file));
            }

            br = new BufferedReader(new InputStreamReader(in, fileEncoding));

            for (int i = 0; i < fromLine; i++) {
                String skipLine = br.readLine();
                LOG.info("Skip line:{}", skipLine);
            }
        } else {
            br = null;
        }
    }

    public void close() throws IOException {
        if (br != null){
            br.close();
            br = null;

            if (ftpHandler instanceof FtpHandler){
                try {
                    ((FtpHandler) ftpHandler).getFtpClient().completePendingCommand();
                } catch (Exception e) {
                    //如果出现了超时异常，就直接获取一个新的ftpHandler
                    LOG.warn("FTPClient completePendingCommand has error ->",e);
                    try{
                        ftpHandler.logoutFtpServer();
                    }catch (Exception exception){
                        LOG.warn("FTPClient logout has error ->",exception);
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

    public void setFileEncoding(String fileEncoding) {
        this.fileEncoding = fileEncoding;
    }
}
