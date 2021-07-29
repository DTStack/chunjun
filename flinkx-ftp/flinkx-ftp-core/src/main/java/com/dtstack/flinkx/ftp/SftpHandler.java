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

package com.dtstack.flinkx.ftp;

import com.dtstack.flinkx.util.ExceptionUtil;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import org.apache.commons.collections.CollectionUtils;
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
public class SftpHandler implements IFtpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SftpHandler.class);

    private Session session = null;

    private ChannelSftp channelSftp = null;

    private static final String DOT = ".";

    private static final String DOT_DOT = "..";

    private static final String SP = "/";

    private static final String SRC_MAIN = "src/main";

    private static String PATH_NOT_EXIST_ERR = "no such file";

    private static String MSG_AUTH_FAIL = "Auth fail";

    @Override
    public void loginFtpServer(FtpConfig ftpConfig) {
        try {
            JSch jsch = new JSch();

            if (StringUtils.isNotEmpty(ftpConfig.getPrivateKeyPath())) {
                // 添加私钥路径
                jsch.addIdentity(ftpConfig.getPrivateKeyPath());
            }

            session = jsch.getSession(ftpConfig.getUsername(), ftpConfig.getHost(), ftpConfig.getPort());
            if (session == null) {
                throw new RuntimeException("login failed. Please check if username and password are correct");
            }

            if(StringUtils.isEmpty(ftpConfig.getPrivateKeyPath())){
                session.setPassword(ftpConfig.getPassword());
            }

            Properties config = new Properties();

            // SSH 公钥检查机制 no、ask、yes
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.setTimeout(ftpConfig.getTimeout());
            session.connect();

            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
        } catch (JSchException e) {
            if(null != e.getCause()){
                String cause = e.getCause().toString();
                String unknownHostException = "java.net.UnknownHostException: " + ftpConfig.getHost();
                String illegalArgumentException = "java.lang.IllegalArgumentException: port out of range:" + ftpConfig.getPort();
                String wrongPort = "java.net.ConnectException: Connection refused";
                if (unknownHostException.equals(cause)) {
                    String message = String.format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器", ftpConfig.getHost());
                    LOG.error(message);
                    throw new RuntimeException(message, e);
                } else if (illegalArgumentException.equals(cause) || wrongPort.equals(cause) ) {
                    String message = String.format("请确认连接ftp服务器端口是否正确，错误的端口: [%s] ", ftpConfig.getPort());
                    LOG.error(message);
                    throw new RuntimeException(message, e);
                }else {
                    LOG.error(ExceptionUtil.getErrorMessage(e));
                    throw new RuntimeException(e);
                }
            }else {
                if(MSG_AUTH_FAIL.equals(e.getMessage())){
                    String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
                            "message:host =" + ftpConfig.getHost() + ",username = " + ftpConfig.getUsername() + ",port =" + ftpConfig.getPort());
                    LOG.error(message);
                    throw new RuntimeException(message, e);
                }else{
                    String message = String.format("与ftp服务器建立连接失败 : [%s]",
                            "message:host =" + ftpConfig.getHost() + ",username = " + ftpConfig.getUsername() + ",port =" + ftpConfig.getPort());
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
            SftpATTRS sftpAttrs = channelSftp.lstat(directoryPath);
            return sftpAttrs.isDir();
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().equals(PATH_NOT_EXIST_ERR)) {
                LOG.warn("{}", e.getMessage());
                return false;
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
            SftpATTRS sftpAttrs = channelSftp.lstat(filePath);
            if(sftpAttrs.getSize() >= 0){
                isExitFlag = true;
            }
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().equals(PATH_NOT_EXIST_ERR)) {
                LOG.warn("{}", e.getMessage());
                return false;
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
    public List<String> listDirs(String path){
        if(StringUtils.isBlank(path)) {
            path = SP;
        }

        List<String> dirs = new ArrayList<>();
        if(isDirExist(path)){
            if(path.equals(DOT) || path.equals(SRC_MAIN)) {
                return dirs;
            }

            if(!path.endsWith(SP)) {
                path = path + SP;
            }

            try {
                Vector vector = channelSftp.ls(path);
                for(int i = 0; i < vector.size(); ++i) {
                    ChannelSftp.LsEntry le = (ChannelSftp.LsEntry) vector.get(i);
                    String strName = le.getFilename();
                    if(!strName.equals(DOT) && !strName.equals(SRC_MAIN) && !strName.equals(DOT_DOT)) {
                        String filePath = path + strName;
                        dirs.add(filePath);
                    }
                }
            } catch (SftpException e) {
                LOG.error("", e);
            }
        }

        return dirs;
    }

    @Override
    public List<String> getFiles(String path) {
        if(StringUtils.isBlank(path)) {
            path = SP;
        }
        List<String> sources = new ArrayList<>();
        if(isDirExist(path)) {
            if(path.equals(DOT) || path.equals(SRC_MAIN)) {
                return sources;
            }
            if(!path.endsWith(SP)) {
                path = path + SP;
            }
            try {
                Vector vector = channelSftp.ls(path);
                for(int i = 0; i < vector.size(); ++i) {
                    ChannelSftp.LsEntry le = (ChannelSftp.LsEntry) vector.get(i);
                    String strName = le.getFilename();
                    if(!strName.equals(DOT) && !strName.equals(SRC_MAIN) && !strName.equals(DOT_DOT)) {
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
            SftpATTRS sftpAttrs = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpAttrs.isDir();
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().equals(PATH_NOT_EXIST_ERR)) {
                LOG.warn("{}", e.getMessage());
                LOG.warn("Path [{}] does not exist and will be created.", directoryPath);
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
            OutputStream writeOutputStream = this.channelSftp.put(filePath, ChannelSftp.APPEND);
            if (null == writeOutputStream) {
                String message = String.format(
                        "打开FTP文件[%s]获取写出流时出错,请确认文件%s有权限创建，有权限写出等", filePath,
                        filePath);
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
            LOG.info("current working directory:{}", this.channelSftp.pwd());
        } catch (Exception e) {
            LOG.warn("printWorkingDirectory error:{}", e.getMessage());
        }
    }

    @Override
    public void deleteAllFilesInDir(String dir, List<String> exclude) {
        if(isDirExist(dir)) {
            if(!dir.endsWith(SP)) {
                dir = dir + SP;
            }

            try {
                Vector vector = channelSftp.ls(dir);
                for(int i = 0; i < vector.size(); ++i) {
                    ChannelSftp.LsEntry le = (ChannelSftp.LsEntry) vector.get(i);
                    String strName = le.getFilename();
                    if(CollectionUtils.isNotEmpty(exclude) && exclude.contains(strName)){
                        continue;
                    }

                    if(!strName.equals(DOT) && !strName.equals(SRC_MAIN) && !strName.equals(DOT_DOT)) {
                        String filePath = dir + strName;
                        deleteAllFilesInDir(filePath, exclude);
                    }
                }

                if(CollectionUtils.isEmpty(exclude)){
                    channelSftp.rmdir(dir);
                }
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
            SftpATTRS sftpAttrs = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpAttrs.isDir();
        } catch (SftpException e) {
            if(!isDirExist){
                LOG.info("Creating a directory step by step [{}]", directoryPath);
                this.channelSftp.mkdir(directoryPath);
                return true;
            }
        }
        if(!isDirExist){
            LOG.info("Creating a directory step by step [{}]", directoryPath);
            this.channelSftp.mkdir(directoryPath);
        }
        return true;
    }

    @Override
    public void rename(String oldPath, String newPath) throws SftpException {
        channelSftp.rename(oldPath, newPath);
    }
}
