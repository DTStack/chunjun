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

package com.dtstack.chunjun.format.excel.client;

import com.dtstack.chunjun.format.excel.config.ExcelFormatConfig;

import com.alibaba.excel.ExcelReader;
import com.alibaba.excel.read.metadata.ReadSheet;

import java.util.ArrayList;
import java.util.List;

public class ExcelReaderExecutor implements Runnable {

    private final ExcelReader reader;
    private ExcelSubExceptionCarrier ec;
    private ExcelFormatConfig config;

    public ExcelReaderExecutor(
            ExcelReader reader, ExcelSubExceptionCarrier ec, ExcelFormatConfig config) {
        this.reader = reader;
        this.ec = ec;
        this.config = config;
    }

    @Override
    public void run() {
        try {
            if (config.getSheetNo() != null) {
                List<ReadSheet> readSheetList = new ArrayList<>();
                for (int i = 0; i < config.getSheetNo().size(); i++) {
                    readSheetList.add(new ReadSheet(config.getSheetNo().get(i)));
                }
                reader.read(readSheetList);
            } else {
                reader.readAll();
            }
        } catch (Exception e) {
            ec.setThrowable(e);
        } finally {
            close();
        }
    }

    private void close() {
        if (reader != null) {
            // Don’t forget to close it here. A temporary file will be
            // created when you read it, and the disk will crash.
            reader.finish();
        }
    }
}
