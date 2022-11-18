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

package com.dtstack.chunjun.connector.file.source;

import com.dtstack.chunjun.config.BaseFileConf;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @program chunjun
 * @author: xiuzhu
 * @create: 2021/06/24
 */
public class FileInputFormat extends BaseRichInputFormat {

    private BaseFileConf fileConf;

    private FileInputBufferedReader fbr;

    private transient String line;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {

        List<String> inputFiles = new ArrayList<>();
        String path = fileConf.getPath();

        if (path != null && path.length() > 0) {
            path = path.replace("\n", "").replace("\r", "");
            String[] filePaths = path.split(",");
            for (String filePath : filePaths) {
                File file = new File(filePath);
                if (file.isFile()) {
                    inputFiles.add(filePath);
                } else if (file.isDirectory()) {
                    File[] childFiles = file.listFiles();
                    List<String> collect =
                            Arrays.asList(childFiles).stream()
                                    .map((f) -> f.getAbsolutePath())
                                    .collect(Collectors.toList());
                    inputFiles.addAll(collect);
                }
            }
        }
        LOG.info("files = {}", GsonUtil.GSON.toJson(inputFiles));
        int numSplits = (Math.min(inputFiles.size(), minNumSplits));
        FileInputSplit[] fileInputSplits = new FileInputSplit[numSplits];
        for (int index = 0; index < numSplits; ++index) {
            fileInputSplits[index] = new FileInputSplit(index);
        }

        for (int i = 0; i < inputFiles.size(); ++i) {
            fileInputSplits[i % numSplits].getPaths().add(inputFiles.get(i));
        }
        return fileInputSplits;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        super.openInputFormat();

        FileInputSplit fileInputSplit = (FileInputSplit) inputSplit;
        List<String> paths = fileInputSplit.getPaths();
        fbr = new FileInputBufferedReader(paths, fileConf);
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            rowData = rowConverter.toInternal(line);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, line);
        }
        return rowData;
    }

    @Override
    protected void closeInternal() throws IOException {
        if (fbr != null) {
            fbr.close();
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        this.line = fbr.readLine();
        return this.line == null;
    }

    public BaseFileConf getFileConf() {
        return fileConf;
    }

    public void setFileConf(BaseFileConf fileConf) {
        this.fileConf = fileConf;
    }
}
