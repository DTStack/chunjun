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

package com.dtstack.chunjun.format.tika.source;

import com.dtstack.chunjun.format.tika.common.TikaData;
import com.dtstack.chunjun.format.tika.config.TikaReadConfig;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.io.Closeable;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Slf4j
public class TikaInputFormat implements Closeable {
    private ThreadPoolExecutor executorService;
    private final BlockingQueue<TikaData> queue = new LinkedBlockingQueue<>(4096);
    private TikaReadConfig tikaReadConfig;
    private TikaData row;
    private int fieldCount;

    public TikaInputFormat(TikaReadConfig tikaReadConfig, int fieldCount) {
        this.tikaReadConfig = tikaReadConfig;
        this.fieldCount = fieldCount;
    }

    public void open(InputStream inputStream, String originalFilename) {
        this.executorService =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0,
                        NANOSECONDS,
                        new LinkedBlockingDeque<>(2),
                        new BasicThreadFactory.Builder()
                                .namingPattern("tika-schedule-pool-%d")
                                .daemon(false)
                                .build());
        TikaReaderExecutor executor =
                new TikaReaderExecutor(tikaReadConfig, queue, inputStream, originalFilename);
        executorService.execute(executor);
    }

    public boolean hasNext() {
        try {
            row = queue.poll(3000L, TimeUnit.MILLISECONDS);
            // 如果没有数据，则继续等待
            if (row == null) {
                log.warn("Waiting for queue get tika data");
                hasNext();
            }
            if (row != null && row.isEnd()) {
                return false;
            }
            return true;
        } catch (InterruptedException e) {
            throw new RuntimeException(
                    "cannot get data from the queue because the current thread is interrupted.", e);
        }
    }

    /** 根据声明的字段个数，对数据进行补全 */
    public String[] nextRecord() {
        String[] data = row.getData();
        if (fieldCount == data.length) {
            return data;
        }
        if (fieldCount < data.length) {
            fieldCount = data.length;
        }
        return formatValue(data);
    }

    private String[] formatValue(String[] data) {
        String[] record = initDataContainer(fieldCount, "");
        // because fieldCount is always >= data.length
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

    @Override
    public void close() {
        if (executorService != null) {
            executorService.shutdown();
            queue.clear();
        }
    }
}
