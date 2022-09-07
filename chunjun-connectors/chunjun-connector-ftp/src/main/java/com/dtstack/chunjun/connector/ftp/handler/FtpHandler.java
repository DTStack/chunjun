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

package com.dtstack.chunjun.connector.ftp.handler;

import com.dtstack.chunjun.connector.ftp.conf.FtpConfig;
import com.dtstack.chunjun.connector.ftp.enums.EFtpMode;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * The concrete Ftp Utility class used for standard ftp
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class FtpHandler implements IFtpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FtpHandler.class);

    private static final String SP = "/";
    private FTPClient ftpClient = null;
    private String controlEncoding;

    public FTPClient getFtpClient() {
        return ftpClient;
    }

    @Override
    public void loginFtpServer(FtpConfig ftpConfig) {
        controlEncoding = ftpConfig.getControlEncoding();
        ftpClient = new FTPClient();
        try {
            // 连接
            ftpClient.connect(ftpConfig.getHost(), ftpConfig.getPort());
            // 登录
            ftpClient.login(ftpConfig.getUsername(), ftpConfig.getPassword());
            // 不需要写死ftp server的OS TYPE,FTPClient getSystemType()方法会自动识别
            ftpClient.setConnectTimeout(ftpConfig.getTimeout());
            ftpClient.setDataTimeout(ftpConfig.getTimeout());
            // 设置控制连接超时
            ftpClient.setSoTimeout(ftpConfig.getTimeout());
            if (EFtpMode.PASV.name().equals(ftpConfig.getConnectPattern())) {
                ftpClient.enterRemotePassiveMode();
                ftpClient.enterLocalPassiveMode();
            } else if (EFtpMode.PORT.name().equals(ftpConfig.getConnectPattern())) {
                ftpClient.enterLocalActiveMode();
            }
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftpClient.disconnect();
                String message =
                        String.format(
                                "与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
                                "message:host ="
                                        + ftpConfig.getHost()
                                        + ",username = "
                                        + ftpConfig.getUsername()
                                        + ",port ="
                                        + ftpConfig.getPort());
                LOG.error(message);
                throw new RuntimeException(message);
            }
            ftpClient.setControlEncoding(ftpConfig.getControlEncoding());
            ftpClient.setListHiddenFiles(ftpConfig.isListHiddenFiles());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void logoutFtpServer() throws IOException {
        if (ftpClient.isConnected()) {
            try {
                ftpClient.logout();
            } finally {
                if (ftpClient.isConnected()) {
                    ftpClient.disconnect();
                }
            }
        }
    }

    @Override
    public boolean isDirExist(String directoryPath) {
        String originDir = null;
        try {

            originDir = ftpClient.printWorkingDirectory();
            ftpClient.enterLocalPassiveMode();
            FTPFile[] ftpFiles = ftpClient.listFiles(encodePath(directoryPath));
            if (ftpFiles.length == 0
                    && !ftpClient.changeWorkingDirectory(encodePath(directoryPath))) {
                return false;
            }
            boolean positiveCompletion =
                    FTPReply.isPositiveCompletion(ftpClient.cwd(encodePath(directoryPath)));
            if (positiveCompletion && ftpFiles.length == 1 && ftpFiles[0].isFile()) {
                String[] split = directoryPath.split(String.valueOf(IOUtils.DIR_SEPARATOR_UNIX));
                // 还要再判断文件名相同 并且如果这是一个文件
                if (ftpFiles[0].getName().equals(split[split.length - 1])) {
                    String ftpFilePath =
                            directoryPath + IOUtils.DIR_SEPARATOR_UNIX + ftpFiles[0].getName();
                    return ftpClient.listFiles(encodePath(ftpFilePath)).length != 0;
                }
            }
            return positiveCompletion;

        } catch (IOException e) {
            String message = String.format("进入目录：[%s]时发生I/O异常,请确认与ftp服务器的连接正常", directoryPath);
            LOG.error(message);
            throw new RuntimeException(message, e);
        } finally {
            if (originDir != null) {
                try {
                    ftpClient.changeWorkingDirectory(originDir);
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }

    @Override
    public boolean isFileExist(String filePath) {
        return !isDirExist(filePath);
    }

    @Override
    public List<String> getFiles(String path) {
        List<String> sources = new ArrayList<>();
        ftpClient.enterLocalPassiveMode();

        if (!isExist(path)) {
            return sources;
        }
        if (isFileExist(path)) {
            sources.add(path);
            return sources;
        } else {
            path = path + SP;
        }
        try {
            listFiles(path, sources);
        } catch (IOException e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        }
        return sources;
    }

    private void listFiles(String path, List<String> sources) throws IOException {
        FTPFile[] ftpFiles = ftpClient.listFiles(encodePath(path));
        if (ftpFiles != null) {
            for (FTPFile ftpFile : ftpFiles) {
                // .和..是特殊文件
                if (StringUtils.endsWith(ftpFile.getName(), ConstantValue.POINT_SYMBOL)
                        || StringUtils.endsWith(
                                ftpFile.getName(), ConstantValue.TWO_POINT_SYMBOL)) {
                    continue;
                }
                sources.addAll(getFiles(path + ftpFile.getName(), ftpFile));
            }
        }
    }

    /**
     * 递归获取指定路径下的所有文件(暂无过滤)
     *
     * @param path path
     * @param file file
     * @return file list
     * @throws IOException io exception.
     */
    private List<String> getFiles(String path, FTPFile file) throws IOException {
        List<String> sources = new ArrayList<>();
        if (file.isDirectory()) {
            if (!path.endsWith(SP)) {
                path = path + SP;
            }
            ftpClient.enterLocalPassiveMode();
            listFiles(path, sources);
        } else {
            sources.add(path);
        }
        LOG.info("path = {}, FTPFile is directory = {}", path, file.isDirectory());
        return sources;
    }

    @Override
    public void mkDirRecursive(String directoryPath) {
        StringBuilder dirPath = new StringBuilder();
        dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
        String[] dirSplit = StringUtils.split(directoryPath, IOUtils.DIR_SEPARATOR_UNIX);
        String message = String.format("创建目录:%s时发生异常,请确认与ftp服务器的连接正常,拥有目录创建权限", directoryPath);
        try {
            // ftp server不支持递归创建目录,只能一级一级创建
            for (String dirName : dirSplit) {
                dirPath.append(dirName);
                boolean mkdirSuccess = mkDirSingleHierarchy(dirPath.toString());
                dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
                if (!mkdirSuccess) {
                    throw new RuntimeException(message);
                }
            }
        } catch (IOException e) {
            message = String.format("%s, errorMessage:%s", message, e.getMessage());
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    private boolean mkDirSingleHierarchy(String directoryPath) throws IOException {
        boolean isDirExist = this.ftpClient.changeWorkingDirectory(encodePath(directoryPath));
        // 如果directoryPath目录不存在,则创建
        if (!isDirExist) {
            int replayCode = this.ftpClient.mkd(encodePath(directoryPath));
            if (replayCode != FTPReply.COMMAND_OK
                    && replayCode != FTPReply.PATHNAME_CREATED
                    && replayCode != FTPReply.FILE_ACTION_OK) {
                LOG.warn(
                        "create path [{}] failed ,replayCode is {} and reply is  {} ",
                        directoryPath,
                        replayCode,
                        ftpClient.getReplyString());
                return false;
            }
        }
        return true;
    }

    @Override
    public OutputStream getOutputStream(String filePath) {
        try {
            this.printWorkingDirectory();
            String parentDir =
                    filePath.substring(
                            0, StringUtils.lastIndexOf(filePath, IOUtils.DIR_SEPARATOR_UNIX));
            this.ftpClient.changeWorkingDirectory(parentDir);
            this.printWorkingDirectory();
            OutputStream writeOutputStream = this.ftpClient.appendFileStream(encodePath(filePath));
            String message =
                    String.format("打开FTP文件[%s]获取写出流时出错,请确认文件%s有权限创建，有权限写出等", filePath, filePath);
            if (null == writeOutputStream) {
                throw new RuntimeException(message);
            }

            return writeOutputStream;
        } catch (IOException e) {
            String message =
                    String.format(
                            "写出文件 : [%s] 时出错,请确认文件:[%s]存在且配置的用户有权限写, errorMessage:%s",
                            filePath, filePath, e.getMessage());
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    private void printWorkingDirectory() {
        try {
            LOG.info(
                    String.format(
                            "current working directory:%s",
                            this.ftpClient.printWorkingDirectory()));
        } catch (Exception e) {
            LOG.warn(String.format("printWorkingDirectory error:%s", e.getMessage()));
        }
    }

    @Override
    public void deleteAllFilesInDir(String dir, List<String> exclude) {
        if (isDirExist(dir)) {
            if (!dir.endsWith(SP)) {
                dir = dir + SP;
            }

            try {
                FTPFile[] ftpFiles = ftpClient.listFiles(encodePath(dir));
                if (ftpFiles != null) {
                    for (FTPFile ftpFile : ftpFiles) {
                        if (CollectionUtils.isNotEmpty(exclude)
                                && exclude.contains(ftpFile.getName())) {
                            continue;
                        }
                        if (StringUtils.endsWith(ftpFile.getName(), ConstantValue.POINT_SYMBOL)
                                || StringUtils.endsWith(
                                        ftpFile.getName(), ConstantValue.TWO_POINT_SYMBOL)) {
                            continue;
                        }
                        deleteAllFilesInDir(dir + ftpFile.getName(), exclude);
                    }
                }

                if (CollectionUtils.isEmpty(exclude)) {
                    ftpClient.rmd(encodePath(dir));
                }
            } catch (IOException e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            }
        } else if (isFileExist(dir)) {
            try {
                ftpClient.deleteFile(encodePath(dir));
            } catch (IOException e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean deleteFile(String filePath) throws IOException {
        try {
            if (isFileExist(filePath)) {
                return ftpClient.deleteFile(filePath);
            }
        } catch (IOException e) {
            throw new IOException(e);
        }
        return true;
    }

    @Override
    public InputStream getInputStream(String filePath) {
        try {
            ftpClient.enterLocalPassiveMode();
            return ftpClient.retrieveFileStream(encodePath(filePath));
        } catch (IOException e) {
            String message =
                    String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    @Override
    public List<String> listDirs(String path) {
        List<String> sources = new ArrayList<>();
        if (isDirExist(path)) {
            if (!path.endsWith(SP)) {
                path = path + SP;
            }

            try {
                FTPFile[] ftpFiles = ftpClient.listFiles(encodePath(path));
                if (ftpFiles != null) {
                    for (FTPFile ftpFile : ftpFiles) {
                        if (StringUtils.endsWith(ftpFile.getName(), ConstantValue.POINT_SYMBOL)
                                || StringUtils.endsWith(
                                        ftpFile.getName(), ConstantValue.TWO_POINT_SYMBOL)) {
                            continue;
                        }
                        sources.add(path + ftpFile.getName());
                    }
                }
            } catch (IOException e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            }
        }

        return sources;
    }

    @Override
    public void rename(String oldPath, String newPath) throws IOException {
        ftpClient.rename(encodePath(oldPath), encodePath(newPath));
    }

    @Override
    public void completePendingCommand() throws IOException {
        try {
            // throw exception when return false
            if (!ftpClient.completePendingCommand()) {
                throw new IOException("I/O error occurs while sending or receiving data");
            }
        } catch (IOException e) {
            LOG.error("I/O error occurs while sending or receiving data");
            throw new IOException(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Override
    public long getFileSize(String path) throws IOException {
        FTPFile[] ftpFiles = ftpClient.listFiles(path);
        if (ftpFiles == null || ftpFiles.length == 0) {
            throw new IOException("file does not exist path: " + path);
        }
        return ftpFiles[0].getSize();
    }

    /**
     * 判断路径是否存在
     *
     * @param path 判断的路径
     * @return true 存在 false 不存在
     */
    private boolean isExist(String path) {
        String originDir = null;
        try {
            originDir = ftpClient.printWorkingDirectory();
            ftpClient.enterLocalPassiveMode();
            FTPFile[] ftpFiles = ftpClient.listFiles(encodePath(path));
            // 空文件夹 或者 不存在的文件夹 length都是0 但是changeWorkingDirectory为true代表是空文件夹
            return ftpFiles.length != 0 || ftpClient.changeWorkingDirectory(encodePath(path));
        } catch (IOException e) {
            String message = String.format("判断：[%s]是否存在时发生I/O异常,请确认与ftp服务器的连接正常", path);
            throw new RuntimeException(message, e);
        } finally {
            if (originDir != null) {
                try {
                    ftpClient.changeWorkingDirectory(originDir);
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }

    private String encodePath(String path) throws UnsupportedEncodingException {
        return new String(path.getBytes(controlEncoding), FTP.DEFAULT_CONTROL_ENCODING);
    }

    @Override
    public void close() throws Exception {
        logoutFtpServer();
    }
}
