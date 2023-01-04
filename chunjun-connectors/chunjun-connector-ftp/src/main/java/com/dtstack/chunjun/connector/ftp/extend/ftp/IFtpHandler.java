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

package com.dtstack.chunjun.connector.ftp.extend.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface IFtpHandler extends AutoCloseable {

    /**
     * 判断给定的目录是否存在
     *
     * @param directoryPath 要检查的路径
     * @return true:存在，false:不存在
     */
    boolean isDirExist(String directoryPath);

    /**
     * 检查给定的文件是否存在
     *
     * @param filePath 要检查的文件路径
     * @return true:存在,false:不存在
     */
    boolean isFileExist(String filePath) throws IOException;

    /**
     * 获取文件输入流
     *
     * @param filePath 文件路径
     * @return 数据流
     */
    InputStream getInputStream(String filePath);

    /**
     * 指定起始位置，获取文件输入流
     *
     * @param filePath 文件路径
     * @param startPosition 指定的位置
     * @return input stream.
     */
    InputStream getInputStreamByPosition(String filePath, long startPosition);

    /**
     * 列出指定路径下的目录
     *
     * @param path 路径
     * @return 目录列表
     */
    long getFileSize(String path) throws IOException;

    /**
     * 列出路径下的文件
     *
     * @param path 路径
     * @return 文件列表
     */
    List<String> getFiles(String path);

    /**
     * 递归创建目录
     *
     * @param directoryPath 要创建的目录
     */
    void mkDirRecursive(String directoryPath);

    /**
     * 获取输出数据流
     *
     * @param filePath 文件路径
     * @return 数据流
     */
    OutputStream getOutputStream(String filePath);

    /**
     * 删除目录下的文件
     *
     * @param dir 指定的目录
     * @param exclude 要排除的文件
     */
    void deleteAllFilesInDir(String dir, List<String> exclude) throws IOException;

    /**
     * 删除文件
     *
     * @param filePath 文件路径
     */
    void deleteFile(String filePath) throws IOException;

    /**
     * 重命名路径
     *
     * @param oldPath 原来的路径名称
     * @param newPath 新的路径名称
     * @throws Exception 可能会出现文件不存在，连接异常等
     */
    void rename(String oldPath, String newPath) throws Exception;
}
