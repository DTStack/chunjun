/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.ftp.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.mortbay.log.Log;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;

/** zip文件流 如果fileNameList不为空 只会读取fileNameList里的文件* */
@Slf4j
public class ZipInputStream extends InputStream {

    private final java.util.zip.ZipInputStream zipInputStream;
    private final List<String> fileNameList;
    private ZipEntry currentZipEntry;

    public ZipInputStream(InputStream in) {
        this.zipInputStream = new java.util.zip.ZipInputStream(in);
        this.fileNameList = new ArrayList<>(12);
    }

    @Override
    public int read() throws IOException {
        // 定位一个Entry数据流的开头
        if (null == this.currentZipEntry) {
            this.currentZipEntry = this.zipInputStream.getNextEntry();
            if (null == this.currentZipEntry) {
                return -1;
            }
        }

        // 不支持zip下的嵌套, 对于目录跳过
        if (this.currentZipEntry.isDirectory()) {
            log.warn(
                    String.format(
                            "meet a directory %s, ignore...", this.currentZipEntry.getName()));
            this.currentZipEntry = null;
            return this.read();
        }

        if (CollectionUtils.isNotEmpty(fileNameList)
                && !fileNameList.contains(currentZipEntry.getName())) {
            if (Log.isDebugEnabled()) {
                log.debug(
                        String.format(
                                "zipEntry with name: %s skip", this.currentZipEntry.getName()));
            }
            this.currentZipEntry = null;
            return this.read();
        }

        // 读取一个Entry数据流
        int result = this.zipInputStream.read();

        // 当前Entry数据流结束了, 需要尝试下一个Entry
        if (-1 == result) {
            this.currentZipEntry = null;
            return this.read();
        } else {
            return result;
        }
    }

    @Override
    public void close() throws IOException {
        this.zipInputStream.close();
    }

    public void addFileName(String fileName) {
        this.fileNameList.add(fileName);
    }
}
