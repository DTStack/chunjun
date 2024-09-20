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

package com.dtstack.chunjun.format.excel.source;

import com.dtstack.chunjun.format.excel.client.ExcelReadListener;
import com.dtstack.chunjun.format.excel.client.ExcelReaderExecutor;
import com.dtstack.chunjun.format.excel.client.ExcelSubExceptionCarrier;
import com.dtstack.chunjun.format.excel.client.Row;
import com.dtstack.chunjun.format.excel.config.ExcelFormatConfig;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelReader;
import com.alibaba.excel.read.builder.ExcelReaderBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.io.Closeable;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.excel.enums.ReadDefaultReturnEnum.ACTUAL_DATA;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Slf4j
public class ExcelInputFormat implements Closeable {

    private BlockingQueue<Row> queue;
    private ThreadPoolExecutor executorService;
    /** The number of cells per row in the Excel file. */
    private Integer cellCount = 0;
    /** The number of sheet in the Excel file. */
    private int sheetNum;

    private ExcelSubExceptionCarrier ec;
    private Row row;

    public void open(InputStream inputStream, ExcelFormatConfig config) {
        this.cellCount = config.getFields().length;
        ExcelReadListener listener = new ExcelReadListener();
        this.queue = listener.getQueue();
        this.ec = new ExcelSubExceptionCarrier();
        ExcelReaderBuilder builder = EasyExcel.read(inputStream, listener);
        if (!config.isFirstLineHeader()) {
            builder.headRowNumber(0);
        }
        builder.ignoreEmptyRow(true);
        builder.autoCloseStream(true);
        // @since 3.2.0
        // STRING:会返回一个Map<Integer,String>的数组，返回值就是你在excel里面不点击单元格看到的内容
        // ACTUAL_DATA：会返回一个Map<Integer,Object>的数组，返回实际上存储的数据，会帮自动转换类型，Object类型为BigDecimal、Boolean、String、LocalDateTime、null，中的一个，
        // READ_CELL_DATA: 会返回一个Map<Integer,ReadCellData<?>>的数组,其中?类型参照ACTUAL_DATA的
        builder.readDefaultReturn(ACTUAL_DATA);
        ExcelReader reader = builder.build();

        this.sheetNum = reader.excelExecutor().sheetList().size();
        this.executorService =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0,
                        NANOSECONDS,
                        new LinkedBlockingDeque<>(2),
                        new BasicThreadFactory.Builder()
                                .namingPattern("excel-schedule-pool-%d")
                                .daemon(false)
                                .build());
        ExcelReaderExecutor executor = new ExcelReaderExecutor(reader, ec, config);
        executorService.execute(executor);
    }

    public boolean hasNext() {
        if (ec.getThrowable() != null) {
            throw new RuntimeException("Read file error.", ec.getThrowable());
        }
        try {
            row = queue.poll(3000L, TimeUnit.MILLISECONDS);

            if (row == null) {
                return false;
            }

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

    public String[] nextRecord() {
        String[] data;
        if (row.isEnd()) {
            try {
                Row head = queue.poll(3000L, TimeUnit.MILLISECONDS);
                if (head != null) {
                    data = head.getData();
                } else {
                    return null;
                }
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
    public void close() {
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
