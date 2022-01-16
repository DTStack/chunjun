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

package com.dtstack.flinkx.connector.ftp.client;

import com.dtstack.flinkx.connector.ftp.client.excel.ExcelReadListener;
import com.dtstack.flinkx.connector.ftp.client.excel.ExcelReaderExceptionHandler;
import com.dtstack.flinkx.connector.ftp.client.excel.ExcelReaderExecutor;
import com.dtstack.flinkx.connector.ftp.client.excel.Row;
import com.dtstack.flinkx.connector.ftp.conf.FtpConfig;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelReader;
import com.alibaba.excel.read.builder.ExcelReaderBuilder;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/** @author by dujie @Description @Date 2021/12/20 */
public class ExcelFileReadClient extends AbstractFileReader {

    private BlockingQueue<Row> queue;

    private ThreadPoolExecutor executorService;

    private int sheetNum;

    private Row row;

    /** The number of cells per row in the Excel file. */
    private Integer cellCount = 0;

    @Override
    public void open(InputStream inputStream, FtpConfig ftpConfig) throws IOException {

        cellCount = ftpConfig.getColumn().size();
        ExcelReadListener listener = new ExcelReadListener();
        queue = listener.getQueue();

        ExcelReaderBuilder builder = EasyExcel.read(inputStream, listener);
        if (!ftpConfig.getIsFirstLineHeader()) {
            builder.headRowNumber(0);
        }
        builder.ignoreEmptyRow(true);
        ExcelReader reader = builder.build();

        sheetNum = reader.excelExecutor().sheetList().size();
        executorService =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0,
                        NANOSECONDS,
                        new LinkedBlockingDeque<>(2),
                        new BasicThreadFactory.Builder()
                                .namingPattern("excel-schedule-pool-%d")
                                .uncaughtExceptionHandler(new ExcelReaderExceptionHandler())
                                .daemon(false)
                                .build());
        ExcelReaderExecutor executor = new ExcelReaderExecutor(reader);
        executorService.execute(executor);
    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            row = queue.take();
            if (row.isEnd()) {
                return row.getSheetIndex() < sheetNum - 1;
            } else {
                return true;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(
                    "cannot get data from the queue because the current thread is interrupted.", e);
        }
    }

    @Override
    public String[] nextRecord() throws IOException {
        String[] data;
        if (row.isEnd()) {
            try {
                data = queue.take().getData();
            } catch (InterruptedException e) {
                throw new RuntimeException(
                        "cannot get data from the queue because the current thread is interrupted.",
                        e);
            }
        } else {
            data = row.getData();
        }
        if (cellCount == data.length) {
            return data;
        }
        if (cellCount < data.length) {
            cellCount = data.length;
        }
        return formatValue(data);
    }

    @Override
    public void close() throws IOException {
        if (executorService != null) {
            executorService.shutdown();
            queue.clear();
        }
    }

    private String[] formatValue(String[] data) {
        String[] record = initDataContainer(cellCount, "");
        // because cellCount is always >= data.length
        System.arraycopy(data, 0, record, 0, data.length);
        return record;
    }

    private String[] initDataContainer(int capacity, String defValue) {
        String[] container = new String[capacity];
        for (int i = 0; i < capacity; i++) {
            container[i] = defValue;
        }
        return container;
    }
}
