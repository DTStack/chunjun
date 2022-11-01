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

package com.dtstack.chunjun.connector.ftp.source;

import java.io.Serializable;
import java.util.StringJoiner;

public class FtpFileSplit implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 这个分片处理文件的开始位置 */
    private long startPosition;

    /** 这个分片处理文件的结束位置 */
    private long endPosition;

    /** 这个分片处理的文件名 */
    private final String filename;

    /** 文件完整路径 */
    private final String fileAbsolutePath;

    /** 压缩类型 */
    private String compressType;

    public FtpFileSplit(
            long startPosition, long endPosition, String fileAbsolutePath, String filename) {
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        this.fileAbsolutePath = fileAbsolutePath;
        this.filename = filename;
    }

    public FtpFileSplit(
            long startPosition,
            long endPosition,
            String fileAbsolutePath,
            String filename,
            String compressType) {
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        this.fileAbsolutePath = fileAbsolutePath;
        this.filename = filename;
        this.compressType = compressType;
    }

    public String getFilename() {
        return this.filename;
    }

    public String getFileAbsolutePath() {
        return this.fileAbsolutePath;
    }

    public long getStartPosition() {
        return this.startPosition;
    }

    public void setStartPosition(long position) {
        this.startPosition = position;
    }

    public long getEndPosition() {
        return this.endPosition;
    }

    public void setEndPosition(long position) {
        this.endPosition = position;
    }

    public String getCompressType() {
        return this.compressType;
    }

    public long getReadLimit() {
        return this.endPosition - this.startPosition;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", FtpFileSplit.class.getSimpleName() + "[", "]")
                .add("startPosition=" + startPosition)
                .add("endPosition=" + endPosition)
                .add("filename='" + filename + "'")
                .add("fileAbsolutePath='" + fileAbsolutePath + "'")
                .add("compressType='" + compressType + "'")
                .toString();
    }
}
