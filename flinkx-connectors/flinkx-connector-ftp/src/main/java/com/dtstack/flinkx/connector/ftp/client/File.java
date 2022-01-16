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

package com.dtstack.flinkx.connector.ftp.client;

import java.io.Serializable;

/** @author by dujie @Description @Date 2021/12/19 */
public class File implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 压缩文件路径 * */
    private String fileCompressPath;
    /** 压缩文件真正位置 解压缩位置* */
    private String fileAbsolutePath;
    /** 解压缩后文件名* */
    private String fileName;
    /** 压缩类型* */
    private String compressType;

    public File(
            String fileCompressPath,
            String fileAbsolutePath,
            String fileName,
            String compressType) {
        this.fileCompressPath = fileCompressPath;
        this.fileAbsolutePath = fileAbsolutePath;
        this.compressType = compressType;
        this.fileName = fileName;
    }

    public String getFileCompressPath() {
        return fileCompressPath;
    }

    public void setFileCompressPath(String fileCompressPath) {
        this.fileCompressPath = fileCompressPath;
    }

    public String getFileAbsolutePath() {
        return fileAbsolutePath;
    }

    public void setFileAbsolutePath(String fileAbsolutePath) {
        this.fileAbsolutePath = fileAbsolutePath;
    }

    public String getCompressType() {
        return compressType;
    }

    public void setCompressType(String compressType) {
        this.compressType = compressType;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public String toString() {
        return "File{"
                + "fileCompressPath='"
                + fileCompressPath
                + '\''
                + ", FileAbsolutePath='"
                + fileAbsolutePath
                + '\''
                + ", fileName='"
                + fileName
                + '\''
                + ", compressType='"
                + compressType
                + '\''
                + '}';
    }
}
