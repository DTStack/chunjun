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

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

/**
 * The concrete Ftp Utility class used for sftp
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class SFtpHandler implements FtpHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SFtpHandler.class);
    Session session = null;
    ChannelSftp channelSftp = null;

    @Override
    public void loginFtpServer(String host, String username, String password, int port, int timeout, String connectMode) {
        JSch jsch = new JSch(); // 创建JSch对象
        try {
            session = jsch.getSession(username, host, port);
            // 根据用户名，主机ip，端口获取一个Session对象
            // 如果服务器连接不上，则抛出异常
            if (session == null) {
                throw new RuntimeException("login failed. Please check if username and password are correct");
            }

            session.setPassword(password); // 设置密码
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config); // 为Session对象设置properties
            session.setTimeout(timeout); // 设置timeout时间
            session.connect(); // 通过Session建立链接

            channelSftp = (ChannelSftp) session.openChannel("sftp"); // 打开SFTP通道
            channelSftp.connect(); // 建立SFTP通道的连接
        } catch (JSchException e) {
            if(null != e.getCause()){
                String cause = e.getCause().toString();
                String unknownHostException = "java.net.UnknownHostException: " + host;
                String illegalArgumentException = "java.lang.IllegalArgumentException: port out of range:" + port;
                String wrongPort = "java.net.ConnectException: Connection refused";
                if (unknownHostException.equals(cause)) {
                    String message = String.format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器", host);
                    LOG.error(message);
                    throw new RuntimeException(message, e);
                } else if (illegalArgumentException.equals(cause) || wrongPort.equals(cause) ) {
                    String message = String.format("请确认连接ftp服务器端口是否正确，错误的端口: [%s] ", port);
                    LOG.error(message);
                    throw new RuntimeException(message, e);
                }
            }else {
                if("Auth fail".equals(e.getMessage())){
                    String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
                            "message:host =" + host + ",username = " + username + ",port =" + port);
                    LOG.error(message);
                    throw new RuntimeException(message, e);
                }else{
                    String message = String.format("与ftp服务器建立连接失败 : [%s]",
                            "message:host =" + host + ",username = " + username + ",port =" + port);
                    LOG.error(message);
                    throw new RuntimeException(message, e);
                }
            }
        }
    }

    @Override
    public void logoutFtpServer() {
        if (channelSftp != null) {
            channelSftp.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }

    @Override
    public boolean isDirExist(String directoryPath) {
        try {
            SftpATTRS sftpATTRS = channelSftp.lstat(directoryPath);
            return sftpATTRS.isDir();
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", directoryPath);
                LOG.error(message);
                throw new RuntimeException(message, e);
            }
            String message = String.format("进入目录：[%s]时发生I/O异常,请确认与ftp服务器的连接正常", directoryPath);
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    @Override
    public boolean isFileExist(String filePath) {
        boolean isExitFlag = false;
        try {
            SftpATTRS sftpATTRS = channelSftp.lstat(filePath);
            if(sftpATTRS.getSize() >= 0){
                isExitFlag = true;
            }
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", filePath);
                LOG.error(message);
                throw new RuntimeException(message, e);
            } else {
                String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
                LOG.error(message);
                throw new RuntimeException(message, e);
            }
        }
        return isExitFlag;
    }

    @Override
    public InputStream getInputStream(String filePath) {
        try {
            return channelSftp.get(filePath);
        } catch (SftpException e) {
            String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    @Override
    public List<String> getFiles(String path) {
        List<String> sources = new ArrayList<>();
        if(isDirExist(path)) {
            if(path.equals(".") || path.equals("src/main")) {
                return sources;
            }
            if(!path.endsWith("/")) {
                path = path + "/";
            }
            try {
                Vector vector = channelSftp.ls(path);
                for(int i = 0; i < vector.size(); ++i) {
                    ChannelSftp.LsEntry le = (ChannelSftp.LsEntry) vector.get(i);
                    String strName = le.getFilename();
                    if(!strName.equals(".") && !strName.equals("src/main")) {
                        String filePath = path + strName;
                        sources.addAll(getFiles(filePath));
                    }
                }
            } catch (SftpException e) {
                LOG.error("", e);
            }

        } else if(isFileExist(path)) {
            sources.add(path);
            return sources;
        }

        return sources;
    }

    @Override
    public void mkDirRecursive(String directoryPath) {
        boolean isDirExist = false;
        try {
            this.printWorkingDirectory();
            SftpATTRS sftpATTRS = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpATTRS.isDir();
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                LOG.warn(String.format(
                        "您的配置项path:[%s]不存在，将尝试进行目录创建, errorMessage:%s",
                        directoryPath, e.getMessage()), e);
                isDirExist = false;
            }
        }
        if (!isDirExist) {
            StringBuilder dirPath = new StringBuilder();
            dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
            String[] dirSplit = StringUtils.split(directoryPath,IOUtils.DIR_SEPARATOR_UNIX);
            try {
                // ftp server不支持递归创建目录,只能一级一级创建
                for(String dirName : dirSplit){
                    dirPath.append(dirName);
                    mkDirSingleHierarchy(dirPath.toString());
                    dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
                }
            } catch (SftpException e) {
                String message = String
                        .format("创建目录:%s时发生I/O异常,请确认与ftp服务器的连接正常,拥有目录创建权限, errorMessage:%s",
                                directoryPath, e.getMessage());
                LOG.error(message, e);
                throw new RuntimeException(message, e);
            }
        }
    }

    @Override
    public OutputStream getOutputStream(String filePath) {
        try {
            this.printWorkingDirectory();
            String parentDir = filePath.substring(0,
                    StringUtils.lastIndexOf(filePath, IOUtils.DIR_SEPARATOR));
            this.channelSftp.cd(parentDir);
            this.printWorkingDirectory();
            OutputStream writeOutputStream = this.channelSftp.put(filePath,
                    ChannelSftp.APPEND);
            String message = String.format(
                    "打开FTP文件[%s]获取写出流时出错,请确认文件%s有权限创建，有权限写出等", filePath,
                    filePath);
            if (null == writeOutputStream) {
                throw new RuntimeException(message);
            }
            return writeOutputStream;
        } catch (SftpException e) {
            String message = String.format(
                    "写出文件[%s] 时出错,请确认文件%s有权限写出, errorMessage:%s", filePath,
                    filePath, e.getMessage());
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    private void printWorkingDirectory() {
        try {
            LOG.info(String.format("current working directory:%s",
                    this.channelSftp.pwd()));
        } catch (Exception e) {
            LOG.warn(String.format("printWorkingDirectory error:%s",
                    e.getMessage()));
        }
    }

    @Override
    public void deleteAllFilesInDir(String dir) {
        if(isDirExist(dir)) {
            if(!dir.endsWith("/")) {
                dir = dir + "/";
            }
            try {
                Vector vector = channelSftp.ls(dir);
                for(int i = 0; i < vector.size(); ++i) {
                    ChannelSftp.LsEntry le = (ChannelSftp.LsEntry) vector.get(i);
                    String strName = le.getFilename();
                    if(!strName.equals(".") && !strName.equals("src/main")) {
                        String filePath = dir + strName;
                        deleteAllFilesInDir(filePath);
                    }
                }
                channelSftp.rmdir(dir);
            } catch (SftpException e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            }
        } else if(isFileExist(dir)) {
            try {
                channelSftp.rm(dir);
            } catch (SftpException e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            }
        }
    }

    public boolean mkDirSingleHierarchy(String directoryPath) throws SftpException {
        boolean isDirExist = false;
        try {
            SftpATTRS sftpATTRS = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpATTRS.isDir();
        } catch (SftpException e) {
            if(!isDirExist){
                LOG.info(String.format("正在逐级创建目录 [%s]",directoryPath));
                this.channelSftp.mkdir(directoryPath);
                return true;
            }
        }
        if(!isDirExist){
            LOG.info(String.format("正在逐级创建目录 [%s]",directoryPath));
            this.channelSftp.mkdir(directoryPath);
        }
        return true;
    }
}
