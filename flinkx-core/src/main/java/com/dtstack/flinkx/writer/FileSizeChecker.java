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


package com.dtstack.flinkx.writer;

import com.dtstack.flinkx.outputformat.FileOutputFormat;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author jiangbo
 * @date 2019/8/28
 */
public class FileSizeChecker {

    private static final String THREAD_NAME = "file-size-checker-thread";

    private long nextNumForCheckDataSize = 1000;

    private long lastWriteSize;

    private FileOutputFormat outputFormat;

    private long period;

    private ScheduledExecutorService scheduledExecutorService;

    public FileSizeChecker(FileOutputFormat outputFormat, long period) {
        this.outputFormat = outputFormat;
        this.period = period;

        scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, THREAD_NAME);
            }
        });
    }

    public void start(){
        scheduledExecutorService.scheduleAtFixedRate(
                this::checkSize,
                0,
                (period * 1000),
                TimeUnit.MILLISECONDS
        );
    }

    private void checkSize(){
        if (outputFormat.getMaxFileSize() <= 0){
            flush();
        }

        if(outputFormat.getNumWriteCounter().getLocalValue() < nextNumForCheckDataSize){
            return;
        }

        if(getCurrentFileSize() > outputFormat.getMaxFileSize()){
            flush();
            lastWriteSize = outputFormat.getBytesWriteCounter().getLocalValue();
        }

        nextNumForCheckDataSize = getNextNumForCheckDataSize();
    }

    private void flush(){
        try {
            outputFormat.flushData();
        } catch (IOException e){
            throw new RuntimeException("Flush data error", e);
        }
    }

    private long getCurrentFileSize(){
        return  (long)(outputFormat.getDeviation() * (outputFormat.getBytesWriteCounter().getLocalValue() - lastWriteSize));
    }

    private long getNextNumForCheckDataSize(){
        long totalBytesWrite = outputFormat.getBytesWriteCounter().getLocalValue();
        long totalRecordWrite = outputFormat.getNumWriteCounter().getLocalValue();

        float eachRecordSize = (totalBytesWrite * outputFormat.getDeviation()) / totalRecordWrite;

        long currentFileSize = getCurrentFileSize();
        long recordNum = (long)((outputFormat.getMaxFileSize() - currentFileSize) / eachRecordSize);

        return totalRecordWrite + recordNum;
    }

    public void stop(){
        if(scheduledExecutorService != null && !scheduledExecutorService.isShutdown() && !scheduledExecutorService.isTerminated()) {
            scheduledExecutorService.shutdown();
        }
    }
}
