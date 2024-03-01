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

package com.dtstack.chunjun.connector.ftp.client.excel;

import com.dtstack.chunjun.util.DateUtil;

import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.read.listener.ReadListener;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ExcelReadListener implements ReadListener<Map<Integer, Object>> {

    private final BlockingQueue<Row> queue = new LinkedBlockingQueue<>(4096);

    @Override
    public void invoke(Map<Integer, Object> data, AnalysisContext context) {
        String[] piece = new String[data.size()];
        for (Map.Entry<Integer, Object> entry : data.entrySet()) {
            String value = "";
            if (entry.getValue() != null) {
                if (entry.getValue() instanceof LocalDateTime) {
                    value =
                            DateUtil.timestampToString(
                                    DateUtil.localDateTimetoDate((LocalDateTime) entry.getValue()));
                } else {
                    value = String.valueOf(entry.getValue());
                }
            }
            piece[entry.getKey()] = value;
        }
        Row row =
                new Row(
                        piece,
                        context.readSheetHolder().getSheetNo(),
                        context.readRowHolder().getRowIndex(),
                        false);
        try {
            queue.put(row);
        } catch (InterruptedException e) {
            throw new RuntimeException(
                    "because the current thread was interrupted, adding data to the queue failed",
                    e);
        }
    }

    @Override
    public void doAfterAllAnalysed(AnalysisContext context) {
        Row row =
                new Row(
                        new String[0],
                        context.readSheetHolder().getSheetNo(),
                        context.readRowHolder().getRowIndex(),
                        true);
        try {
            queue.put(row);
        } catch (InterruptedException e) {
            throw new RuntimeException(
                    "because the current thread was interrupted, adding data to the queue failed",
                    e);
        }
    }

    public BlockingQueue<Row> getQueue() {
        return queue;
    }
}
