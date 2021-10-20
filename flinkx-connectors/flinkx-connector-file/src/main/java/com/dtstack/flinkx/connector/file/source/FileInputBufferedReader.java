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

package com.dtstack.flinkx.connector.file.source;

import com.dtstack.flinkx.conf.BaseFileConf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Iterator;
import java.util.List;

/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/06/24
 */
public class FileInputBufferedReader {

    private Iterator<String> pathIterator;

    private LineNumberReader lr;

    private BaseFileConf fileConf;

    private boolean hasNext = true;

    public FileInputBufferedReader(List<String> paths, BaseFileConf fileConf) {
        this.fileConf = fileConf;
        pathIterator = paths.iterator();
    }

    public String readLine() throws IOException {

        String line = null;
        if (lr == null) {
            nextFileStream();
        }

        if (!hasNext) {
            return line;
        }

        if (lr != null) {
            if (lr.getLineNumber() < fileConf.getFromLine()) {
                while (lr.getLineNumber() < fileConf.getFromLine()) {
                    line = lr.readLine();
                }
            } else {
                line = lr.readLine();
            }
        }

        if (line == null) {
            close();
            return readLine();
        }
        return line;
    }

    public void nextFileStream() throws IOException {
        if (pathIterator.hasNext()) {
            String filePath = pathIterator.next();
            String encoding = fileConf.getEncoding();
            InputStreamReader isr =
                    new InputStreamReader(new FileInputStream(new File(filePath)), encoding);
            lr = new LineNumberReader(isr);
        } else {
            lr = null;
            hasNext = false;
        }
    }

    public void close() throws IOException {
        if (lr != null) {
            lr.close();
            lr = null;
        }
    }
}
