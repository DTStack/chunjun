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

package com.dtstack.chunjun.connector.ftp.sink;

import com.dtstack.chunjun.connector.ftp.config.FtpConfig;
import com.dtstack.chunjun.connector.ftp.enums.CompressType;
import com.dtstack.chunjun.connector.ftp.handler.DTFtpHandler;
import com.dtstack.chunjun.connector.ftp.handler.FtpHandlerFactory;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.enums.SizeUnitType;
import com.dtstack.chunjun.sink.WriteMode;
import com.dtstack.chunjun.sink.format.BaseFileOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
public class FtpOutputFormat extends BaseFileOutputFormat {

    /** 换行符 */
    private static final int NEWLINE = 10;

    private static final String SP = "/";

    private static final long serialVersionUID = -3331910729974811530L;

    protected FtpConfig ftpConfig;

    private transient DTFtpHandler ftpHandler;

    private transient BufferedWriter writer;

    private transient OutputStream os;

    private boolean isCompress = false;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        super.openInternal(taskNumber, numTasks);
        ftpHandler = FtpHandlerFactory.createFtpHandler(ftpConfig.getProtocol());
        ftpHandler.loginFtpServer(ftpConfig);
        if (StringUtils.isNotEmpty(ftpConfig.getCompressType())) {
            isCompress = true;
        }
    }

    @Override
    protected void openSource() {}

    @Override
    protected void checkOutputDir() {
        wrapFtpHandler(
                iFtpHandler -> {
                    if (iFtpHandler.isDirExist(tmpPath)) {
                        if (iFtpHandler.isFileExist(tmpPath)) {
                            throw new ChunJunRuntimeException(
                                    String.format("dir:[%s] is a file", tmpPath));
                        }
                    } else {
                        iFtpHandler.mkDirRecursive(tmpPath);
                    }
                });
    }

    @Override
    protected void deleteDataDir() {
        wrapFtpHandler(iFtpHandler -> iFtpHandler.deleteAllFilesInDir(outputFilePath, null));
    }

    @Override
    protected void deleteTmpDataDir() {
        wrapFtpHandler(iFtpHandler -> iFtpHandler.deleteAllFilesInDir(tmpPath, null));
    }

    @Override
    protected void nextBlock() {
        super.nextBlock();
        if (isCompress) {
            if (os != null) {
                return;
            }
        } else {
            if (writer != null) {
                return;
            }
        }
        String currentBlockTmpPath = tmpPath + SP + currentFileName;
        try {
            os = ftpHandler.getOutputStream(currentBlockTmpPath);
            if (isCompress) {
                if (ftpConfig.getCompressType().equalsIgnoreCase(CompressType.GZIP.name())) {
                    os = new GzipCompressorOutputStream(os);
                } else if (ftpConfig
                        .getCompressType()
                        .equalsIgnoreCase(CompressType.BZIP2.name())) {
                    os = new BZip2CompressorOutputStream(os);
                } else {
                    throw new UnsupportedTypeException(
                            String.format(
                                    "Unsupported compress type:[%s]", ftpConfig.getCompressType()));
                }
            } else {
                writer =
                        new BufferedWriter(
                                new OutputStreamWriter(os, ftpConfig.getEncoding()), 131072);
            }
            log.info("subtask:[{}] create block file:{}", taskNumber, currentBlockTmpPath);
        } catch (IOException e) {
            throw new ChunJunRuntimeException(ExceptionUtil.getErrorMessage(e));
        }

        currentFileIndex++;
    }

    @Override
    protected void checkCurrentFileSize() {
        if (numWriteCounter.getLocalValue() < nextNumForCheckDataSize) {
            return;
        }
        if (!isCompress) {
            try {
                // Does not manually flush cause a message error?
                writer.flush();
            } catch (IOException e) {
                throw new ChunJunRuntimeException("flush failed when check fileSize");
            }
        }
        long currentFileSize = getCurrentFileSize();
        if (currentFileSize > ftpConfig.getMaxFileSize()) {
            flushData();
        }
        nextNumForCheckDataSize += ftpConfig.getNextCheckRows();
        log.info(
                "current file: {}, size = {}, nextNumForCheckDataSize = {}",
                currentFileName,
                SizeUnitType.readableFileSize(currentFileSize),
                nextNumForCheckDataSize);
    }

    @Override
    public void writeSingleRecordToFile(RowData rowData) throws WriteRecordException {
        try {
            String line = (String) rowConverter.toExternal(rowData, "");
            if (isCompress) {
                if (os == null) {
                    nextBlock();
                }
                byte[] bytes = line.getBytes(ftpConfig.getEncoding());
                this.os.write(bytes);
                this.os.write(NEWLINE);
            } else {
                if (writer == null) {
                    nextBlock();
                }
                this.writer.write(line);
                this.writer.write(NEWLINE);
            }
            lastRow = rowData;
        } catch (Exception ex) {
            throw new WriteRecordException(ex.getMessage(), ex, 0, rowData);
        }
    }

    @Override
    public void closeInternal() throws IOException {
        super.closeInternal();
        try {
            if (isCompress) {
                if (os != null) {
                    os.flush();
                    os.close();
                    os = null;
                }
            } else {
                if (writer != null) {
                    writer.flush();
                    writer.close();
                    writer = null;
                }
            }
            this.ftpHandler.logoutFtpServer();
        } catch (Exception e) {
            throw new ChunJunRuntimeException("can't close source.", e);
        } finally {
            this.ftpHandler.logoutFtpServer();
        }
    }

    @Override
    protected void closeSource() {
        try {
            if (writer != null) {
                writer.flush();
                writer.close();
                writer = null;
            }

            if (os != null) {
                os.flush();
                os.close();
                os = null;
            }
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    @Override
    public void flushDataInternal() {
        if (isCompress) {
            log.info(
                    "Close current text stream, write data size:[{}]",
                    SizeUnitType.readableFileSize(bytesWriteCounter.getLocalValue()));
            try {
                if (os != null) {
                    os.flush();
                    os.close();
                    os = null;
                }
            } catch (IOException e) {
                throw new ChunJunRuntimeException(
                        "error to flush stream." + ExceptionUtil.getErrorMessage(e), e);
            }
        } else {
            closeSource();
        }
    }

    @Override
    protected List<String> copyTmpDataFileToDir() {
        String filePrefix = jobId + "_" + taskNumber;
        String currentFilePath = "";
        List<String> copyList = new ArrayList<>();
        try {
            List<String> dataFiles = ftpHandler.getFiles(tmpPath);
            for (String dataFile : dataFiles) {
                File tmpDataFile = new File(dataFile);
                if (!tmpDataFile.getName().startsWith(filePrefix)) {
                    continue;
                }

                currentFilePath = dataFile;
                String fileName =
                        handleUserSpecificFileName(
                                tmpDataFile.getName(), dataFiles.size(), copyList, ftpHandler);
                String newFilePath = outputFilePath + SP + fileName;
                ftpHandler.rename(currentFilePath, newFilePath);
                copyList.add(newFilePath);
                log.info("copy temp file:{} to dir:{}", currentFilePath, outputFilePath);
            }
        } catch (Exception e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "can't copy temp file:[%s] to dir:[%s]",
                            currentFilePath, outputFilePath),
                    e);
        }
        return copyList;
    }

    @Override
    protected void deleteDataFiles(List<String> preCommitFilePathList, String path) {
        String currentFilePath = "";
        try {
            for (String filePath : this.preCommitFilePathList) {
                if (org.apache.commons.lang3.StringUtils.equals(path, outputFilePath)) {
                    ftpHandler.deleteFile(filePath);
                    log.info("delete file:{}", currentFilePath);
                }
            }
        } catch (IOException e) {
            throw new ChunJunRuntimeException(
                    String.format("can't delete commit file:[%s]", currentFilePath), e);
        }
    }

    @Override
    protected void moveAllTmpDataFileToDir() {
        wrapFtpHandler(
                (DTFtpHandler handler) -> {
                    String dataFilePath = "";
                    try {
                        List<String> dataFiles = handler.getFiles(tmpPath);
                        List<String> copyList = new ArrayList<>();
                        for (String dataFile : dataFiles) {
                            File tmpDataFile = new File(dataFile);
                            dataFilePath = dataFile;

                            String fileName =
                                    handleUserSpecificFileName(
                                            tmpDataFile.getName(),
                                            dataFiles.size(),
                                            copyList,
                                            handler);
                            String newDataFilePath = ftpConfig.getPath() + SP + fileName;
                            handler.rename(dataFilePath, newDataFilePath);
                            copyList.add(newDataFilePath);
                            log.info("move temp file:{} to dir:{}", dataFilePath, outputFilePath);
                        }
                        handler.deleteAllFilesInDir(tmpPath, null);
                    } catch (Exception e) {
                        throw new ChunJunRuntimeException(
                                String.format(
                                        "can't copy temp file:[%s] to dir:[%s]",
                                        dataFilePath, outputFilePath),
                                e);
                    }
                });
    }

    @Override
    public float getDeviation() {
        return 1.0F;
    }

    @Override
    protected String getExtension() {
        if (StringUtils.isNotBlank(ftpConfig.getFileType())) {
            return ftpConfig.getFileType();
        }
        return "";
    }

    @Override
    protected long getCurrentFileSize() {
        String path = tmpPath + SP + currentFileName;
        try {
            return ftpHandler.getFileSize(path);
        } catch (IOException e) {
            throw new ChunJunRuntimeException("can't get file size from ftp, file = " + path, e);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        throw new UnsupportedOperationException("FtpWriter不支持批量写入");
    }

    public FtpConfig getFtpConfig() {
        return ftpConfig;
    }

    public void setFtpConfig(FtpConfig ftpConfig) {
        this.ftpConfig = ftpConfig;
    }

    private String handleUserSpecificFileName(
            String tmpDataFileName, int fileNumber, List<String> copyList, DTFtpHandler handler)
            throws IOException {
        String fileName = ftpConfig.getFtpFileName();
        if (StringUtils.isNotBlank(fileName)) {
            if (fileNumber == 1) {
                fileName = ftpConfig.getFtpFileName();
            } else {
                fileName = handlerMultiChannel(tmpDataFileName);
            }
        } else {
            fileName = tmpDataFileName;
        }
        return filepathCheck(fileName, copyList, handler);
    }

    private String handlerMultiChannel(String tmpDataFileName) {
        final String[] splitFileName = tmpDataFileName.split("_");
        String fileName = ftpConfig.getFtpFileName();
        if (fileName.contains(".")) {
            final int dotPosition = fileName.lastIndexOf(ConstantValue.POINT_SYMBOL);
            final String prefixName = fileName.substring(0, dotPosition);
            final String extensionName = fileName.substring(dotPosition + 1);
            fileName =
                    prefixName
                            + "_"
                            + splitFileName[1]
                            + "_"
                            + splitFileName[2]
                            + "."
                            + extensionName;
        } else {
            fileName = fileName + "_" + splitFileName[1] + "_" + splitFileName[2];
        }
        return fileName;
    }

    private String filepathCheck(String filename, List<String> copyList, DTFtpHandler handler)
            throws IOException {
        if (WriteMode.NONCONFLICT.name().equalsIgnoreCase(ftpConfig.getWriteMode())) {
            if (handler.isFileExist(ftpConfig.getPath() + SP + filename)) {
                handler.deleteAllFilesInDir(tmpPath, null);
                if (!copyList.isEmpty()) {
                    for (String filePath : copyList) {
                        try {
                            handler.deleteFile(filePath);
                        } catch (IOException e) {
                            throw new ChunJunRuntimeException(
                                    "Failed to rollback file,errMsg:" + e.getMessage());
                        }
                    }
                }
                throw new ChunJunRuntimeException(
                        String.format("the file: %s already exists", filename));
            }
        } else if (WriteMode.INSERT.name().equalsIgnoreCase(ftpConfig.getWriteMode())) {
            if (handler.isFileExist(ftpConfig.getPath() + SP + filename)) {
                String suffix =
                        StringUtils.isNotBlank(ftpConfig.getSuffix())
                                ? "_" + ftpConfig.getSuffix()
                                : "_" + UUID.randomUUID();
                StringBuilder sb = new StringBuilder(filename);
                filename =
                        filename.contains(".")
                                ? sb.insert(
                                                filename.lastIndexOf(ConstantValue.POINT_SYMBOL),
                                                suffix)
                                        .toString()
                                : sb.append(suffix).toString();
            }
        }
        return filename;
    }

    private void wrapFtpHandler(Callback<DTFtpHandler> callback) {
        try (DTFtpHandler tmpFtpHandler =
                FtpHandlerFactory.createFtpHandler(ftpConfig.getProtocol())) {
            tmpFtpHandler.loginFtpServer(ftpConfig);
            callback.apply(tmpFtpHandler);
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    @FunctionalInterface
    interface Callback<T> {
        void apply(T t) throws IOException;
    }
}
