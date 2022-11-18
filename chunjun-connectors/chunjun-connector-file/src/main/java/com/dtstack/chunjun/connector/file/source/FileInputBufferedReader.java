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

import com.dtstack.chunjun.config.BaseFileConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;

public class FileInputBufferedReader {

    private final Iterator<String> pathIterator;

    private final BaseFileConfig fileConfig;

    private LineNumberReader lr;

    private boolean hasNext = true;

    public FileInputBufferedReader(List<String> paths, BaseFileConfig fileConfig) {
        this.fileConfig = fileConfig;
        pathIterator = paths.iterator();
    }

    public String readLine() throws IOException {

        String line = null;
        if (lr == null) {
            nextFileStream();
        }

        if (!hasNext) {
            return null;
        }

        if (lr != null) {
            if (lr.getLineNumber() < fileConfig.getFromLine()) {
                while (lr.getLineNumber() < fileConfig.getFromLine()) {
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
            String encoding = fileConfig.getEncoding();
            InputStreamReader isr =
                    new InputStreamReader(
                            Files.newInputStream(new File(filePath).toPath()), encoding);
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
