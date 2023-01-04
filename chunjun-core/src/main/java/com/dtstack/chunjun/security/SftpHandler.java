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

package com.dtstack.chunjun.security;

import com.dtstack.chunjun.util.RetryUtil;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

@Slf4j
public class SftpHandler {

    private static final String KEY_USERNAME = "username";
    private static final String KEY_PASSWORD = "password";
    private static final String KEY_HOST = "host";
    private static final String KEY_PORT = "port";
    private static final String KEY_TIMEOUT = "timeout";

    private static final String KEYWORD_FILE_NOT_EXISTS = "No such file";

    private static final int DEFAULT_HOST = 22;

    private final Session session;
    private final ChannelSftp channelSftp;

    private SftpHandler(Session session, ChannelSftp channelSftp) {
        this.session = session;
        this.channelSftp = channelSftp;
    }

    public static SftpHandler getInstanceWithRetry(Map<String, String> sftpConfig) {
        try {
            return RetryUtil.executeWithRetry(() -> getInstance(sftpConfig), 3, 1000, false);
        } catch (Exception e) {
            throw new RuntimeException("获取SFTPHandler出错", e);
        }
    }

    private static SftpHandler getInstance(Map<String, String> sftpConfig) {
        checkConfig(sftpConfig);

        String host = MapUtils.getString(sftpConfig, KEY_HOST);
        int port = MapUtils.getIntValue(sftpConfig, KEY_PORT, DEFAULT_HOST);
        String username = MapUtils.getString(sftpConfig, KEY_USERNAME);

        try {
            JSch jsch = new JSch();
            Session session = jsch.getSession(username, host, port);
            if (session == null) {
                throw new RuntimeException(
                        "Login failed. Please check if username and password are correct");
            }

            session.setPassword(MapUtils.getString(sftpConfig, KEY_PASSWORD));
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.setTimeout(MapUtils.getIntValue(sftpConfig, KEY_TIMEOUT, 0));
            session.connect();

            ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            return new SftpHandler(session, channelSftp);
        } catch (Exception e) {
            String message =
                    String.format(
                            "与ftp服务器建立连接失败 : [%s]",
                            "message:host =" + host + ",username = " + username + ",port =" + port);
            throw new RuntimeException(message, e);
        }
    }

    private static void checkConfig(Map<String, String> sftpConfig) {
        if (sftpConfig == null || sftpConfig.isEmpty()) {
            throw new IllegalArgumentException("The config of sftp is null");
        }

        if (StringUtils.isEmpty(sftpConfig.get(KEY_HOST))) {
            throw new IllegalArgumentException("The host of sftp is null");
        }
    }

    public void downloadFileWithRetry(String ftpPath, String localPath) {
        try {
            RetryUtil.executeWithRetry(
                    new Callable<Object>() {
                        @Override
                        public Object call() throws Exception {
                            downloadFile(ftpPath, localPath);
                            return null;
                        }
                    },
                    3,
                    1000,
                    false);
        } catch (Exception e) {
            throw new IllegalArgumentException("下载文件失败", e);
        }
    }

    private void downloadFile(String ftpPath, String localPath) {
        if (!isFileExist(ftpPath)) {
            throw new RuntimeException("File not exist on sftp:" + ftpPath);
        }

        try (OutputStream os = new FileOutputStream(new File(localPath))) {
            channelSftp.get(ftpPath, os);
            os.flush();
        } catch (Exception e) {
            throw new RuntimeException("download file from sftp error", e);
        }
    }

    public boolean isFileExist(String ftpPath) {
        try {
            channelSftp.lstat(ftpPath);
            return true;
        } catch (SftpException e) {
            if (e.getMessage().contains(KEYWORD_FILE_NOT_EXISTS)) {
                return false;
            } else {
                throw new RuntimeException("Check file exists error", e);
            }
        }
    }

    public void close() {
        if (channelSftp != null) {
            channelSftp.disconnect();
        }

        if (session != null) {
            session.disconnect();
        }
    }
}
