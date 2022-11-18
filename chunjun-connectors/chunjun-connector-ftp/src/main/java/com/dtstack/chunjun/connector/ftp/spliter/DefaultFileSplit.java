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
import com.dtstack.chunjun.connector.ftp.extend.ftp.concurrent.ConcurrentFileSplit;
import com.dtstack.chunjun.connector.ftp.extend.ftp.concurrent.FtpFileSplit;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class DefaultFileSplit implements ConcurrentFileSplit {
    @Override
    public List<FtpFileSplit> buildFtpFileSplit(
            IFtpHandler handler, IFormatConfig config, List<String> files) {

        List<FtpFileSplit> ftpFileSplits = new ArrayList<>();

        for (String filePath : files) {
            try {
                String fileName = FileUtil.getFilename(filePath);
                long currentFileSize = handler.getFileSize(filePath);
                FtpFileSplit fileSplit = new FtpFileSplit(0, currentFileSize, filePath, fileName);
                ftpFileSplits.add(fileSplit);
            } catch (Exception e) {
                throw new ChunJunRuntimeException(e);
            }
        }

        ftpFileSplits.sort(compare());
        return ftpFileSplits;
    }

    /**
     * 先根据文件路径排序 再根据文件里面开始的偏移量排序, 从小到大
     *
     * @return
     */
    @Override
    public Comparator<FtpFileSplit> compare() {
        return Comparator.comparing(FtpFileSplit::getFileAbsolutePath)
                .thenComparing(FtpFileSplit::getStartPosition, Long::compareTo);
    }
}
