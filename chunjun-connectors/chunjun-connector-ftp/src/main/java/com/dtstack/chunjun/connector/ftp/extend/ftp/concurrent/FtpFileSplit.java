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

package com.dtstack.chunjun.connector.ftp.extend.ftp.concurrent;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@AllArgsConstructor
@Data
public class FtpFileSplit implements Serializable {

    private static final long serialVersionUID = -5408358718251478998L;

    /** 这个分片处理文件的开始位置 */
    private long startPosition = 0;

    /** 这个分片处理文件的结束位置 */
    private long endPosition = 0;

    /** 这个分片处理的文件名 */
    private String filename;

    /** 文件完整路径 */
    private String fileAbsolutePath;

    /** 压缩类型 */
    private String compressType;

    public FtpFileSplit(
            long startPosition, long endPosition, String fileAbsolutePath, String filename) {
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        this.fileAbsolutePath = fileAbsolutePath;
        this.filename = filename;
    }

    public long getReadLimit() {
        return endPosition - startPosition;
    }
}
