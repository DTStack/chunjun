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

package com.dtstack.chunjun.connector.ftp.iformat;

import com.dtstack.chunjun.connector.ftp.extend.ftp.File;
import com.dtstack.chunjun.connector.ftp.extend.ftp.IFormatConfig;
import com.dtstack.chunjun.connector.ftp.extend.ftp.format.IFileReadFormat;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

@Slf4j
public class TextFileFormat implements IFileReadFormat {

    private BufferedReader bufferedReader;
    private String filedDelimiter;
    private String line;

    @Override
    public void open(File file, InputStream inputStream, IFormatConfig config) throws IOException {
        log.info("open file : {}", file.getFileName());
        this.bufferedReader =
                new BufferedReader(new InputStreamReader(inputStream, config.getEncoding()));
        this.filedDelimiter = config.getFieldDelimiter();
    }

    @Override
    public boolean hasNext() throws IOException {
        line = bufferedReader.readLine();
        return line != null;
    }

    @Override
    public String[] nextRecord() {
        return StringUtils.splitByWholeSeparatorPreserveAllTokens(line, filedDelimiter);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(bufferedReader);
    }
}
