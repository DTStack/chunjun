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

package com.dtstack.chunjun.connector.ftp.spliter;

import com.dtstack.chunjun.connector.ftp.client.FileUtil;
import com.dtstack.chunjun.connector.ftp.extend.ftp.IFormatConfig;
import com.dtstack.chunjun.connector.ftp.extend.ftp.IFtpHandler;
import com.dtstack.chunjun.connector.ftp.extend.ftp.concurrent.FtpFileSplit;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ConcurrentCsvSplit extends DefaultFileSplit {

    @Override
    public List<FtpFileSplit> buildFtpFileSplit(
            IFtpHandler handler, IFormatConfig config, List<String> files) {
        List<FtpFileSplit> fileSplits = new ArrayList<>();

        for (String filePath : files) {
            fileSplits.addAll(analyseSingleFile(handler, config, filePath));
        }
        return fileSplits;
    }

    public List<FtpFileSplit> analyseSingleFile(
            IFtpHandler ftpHandler, IFormatConfig config, String filePath) {
        List<FtpFileSplit> ftpFileSplits = new ArrayList<>();

        String columnDelimiter = config.getColumnDelimiter();
        try {
            long currentFileSize = ftpHandler.getFileSize(filePath);
            long fetchSize = Math.max(1024 * 1024, config.getFetchMaxSize());
            String filename = FileUtil.getFilename(filePath);
            if (currentFileSize > fetchSize) {
                int perSplit =
                        Math.min(
                                (int) currentFileSize / config.getParallelism(), Integer.MAX_VALUE);
                long startPosition = 0;
                long endPosition = startPosition + perSplit;

                while (endPosition <= currentFileSize) {
                    if (endPosition == currentFileSize) {
                        FtpFileSplit ftpFileSplit =
                                new FtpFileSplit(startPosition, endPosition, filePath, filename);
                        ftpFileSplits.add(ftpFileSplit);
                        break;
                    }

                    InputStream input = ftpHandler.getInputStreamByPosition(filePath, endPosition);
                    boolean notMatch = true;

                    while (notMatch) {
                        notMatch = false;
                        for (char c : columnDelimiter.toCharArray()) {
                            char ch = (char) input.read();
                            endPosition += 1;

                            if (ch != c) {
                                notMatch = true;
                                break;
                            }
                        }
                    }

                    FtpFileSplit ftpFileSplit =
                            new FtpFileSplit(startPosition, endPosition, filePath, filename);
                    ftpFileSplits.add(ftpFileSplit);

                    log.info(
                            String.format(
                                    "build file split, filename: %s, startPosition: %d, endPosition: %d",
                                    filePath, startPosition, endPosition));

                    startPosition = endPosition;
                    endPosition = startPosition + perSplit;
                }

                if (endPosition != currentFileSize) {
                    FtpFileSplit ftpFileSplit =
                            new FtpFileSplit(startPosition, currentFileSize, filePath, filename);
                    ftpFileSplits.add(ftpFileSplit);

                    log.info(
                            String.format(
                                    "build file split, filename: %s, startPosition: %d, endPosition: %d",
                                    filePath, startPosition, currentFileSize));
                }
            } else {
                ftpFileSplits.add(new FtpFileSplit(0, currentFileSize, filePath, filename));
            }

            ftpFileSplits.sort(compare());

            return ftpFileSplits;
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }
}
