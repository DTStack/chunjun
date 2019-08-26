/**
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


package com.dtstack.flinkx.ftp;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * The skeleton of Ftp Utility class
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public interface IFtpHandler {
    /**
     *
     * @Title: LoginFtpServer
     * @Description: 与ftp服务器建立连接
     * @param @param host
     * @param @param username
     * @param @param password
     * @param @param port
     * @param @param timeout
     * @param @param connectMode
     * @return void
     * @throws
     */
    void loginFtpServer(String host, String username, String password, int port, int timeout,String connectMode) ;
    /**
     *
     * @Title: LogoutFtpServer
     * @Description: 断开与ftp服务器的连接
     * @param
     * @return void
     * @throws
     */
    void logoutFtpServer();
    /**
     *
     * @Title: isDirExist
     * @Description: 判断指定路径是否是目录
     * @param @param directoryPath
     * @param @return
     * @return boolean
     * @throws
     */
    boolean isDirExist(String directoryPath);
    /**
     *
     * @Title: isFileExist
     * @Description: 判断指定路径是否是文件
     * @param @param filePath
     * @param @return
     * @return boolean
     * @throws
     */
    boolean isFileExist(String filePath);

    /**
     *
     * @Title: getInputStream
     * @Description: 获取指定路径的输入流
     * @param @param filePath
     * @param @return
     * @return InputStream
     * @throws
     */
    InputStream getInputStream(String filePath);

    List<String> listDirs(String path);

    List<String> getFiles(String path);

    void mkDirRecursive(String directoryPath);

    OutputStream getOutputStream(String filePath);

    void deleteAllFilesInDir(String dir, List<String> exclude);

    void rename(String oldPath, String newPath) throws Exception;
}
