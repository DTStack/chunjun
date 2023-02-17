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

import com.csvreader.CsvReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

@Slf4j
public class CsvFileFormat implements IFileReadFormat {

    private CsvReader csvReader;
    private BufferedReader bufferedReader;

    @Override
    public void open(File file, InputStream inputStream, IFormatConfig config) throws IOException {
        log.info("open file : {}", file.getFileName());
        bufferedReader =
                new BufferedReader(new InputStreamReader(inputStream, config.getEncoding()));
        csvReader = new CsvReader(bufferedReader);
        csvReader.setDelimiter(config.getFieldDelimiter().charAt(0));

        if (MapUtils.isNotEmpty(config.getFileConfig())) {
            Map<String, Object> csvConfig = config.getFileConfig();
            // 是否跳过空行
            csvReader.setSkipEmptyRecords(
                    (Boolean) csvConfig.getOrDefault("skipEmptyRecords", true));
            // 是否使用csv转义字符
            csvReader.setUseTextQualifier(
                    (Boolean) csvConfig.getOrDefault("useTextQualifier", true));
            csvReader.setTrimWhitespace((Boolean) csvConfig.getOrDefault("trimWhitespace", false));
            // 单列长度是否限制100000字符
            csvReader.setSafetySwitch((Boolean) csvConfig.getOrDefault("safetySwitch", false));
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return csvReader.readRecord();
    }

    @Override
    public String[] nextRecord() throws IOException {
        return csvReader.getValues();
    }

    @Override
    public void close() throws IOException {
        csvReader.close();
        IOUtils.closeQuietly(bufferedReader);
    }
}
