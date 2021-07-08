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

import com.dtstack.flinkx.ftp.IFtpHandler;
import com.dtstack.flinkx.ftp.FtpHandler;

import java.io.*;
import java.util.Iterator;

/**
 * The FtpSeqBufferedReader that read multiple ftp files one by one.
 *
 * @author jiangbo
 * @date 2018/12/18
 */
public class FtpSeqBufferedReader {

    private IFtpHandler ftpHandler;

    private Iterator<String> iter;

    private int fromLine = 0;

    private BufferedReader br;

    private String charsetName = "utf-8";

    public FtpSeqBufferedReader(IFtpHandler ftpHandler, Iterator<String> iter) {
        this.ftpHandler = ftpHandler;
        this.iter = iter;
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
                throw new NullPointerException();
            }

            br = new BufferedReader(new InputStreamReader(in, charsetName));

            for (int i = 0; i < fromLine; i++) {
                br.readLine();
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
                ((FtpHandler) ftpHandler).getFtpClient().completePendingCommand();
            }
        }
    }

    public void setFromLine(int fromLine) {
        this.fromLine = fromLine;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }
}
