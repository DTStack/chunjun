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

package com.dtstack.flinkx.reader;

import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.metrics.AccumulatorCollector;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * This class is user for speed control
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ByteRateLimiter {

    private final static Logger LOG = LoggerFactory.getLogger(ByteRateLimiter.class);

    public static final int MIN_RECORD_NUMBER_UPDATE_RATE = 1000;

    private RateLimiter rateLimiter;

    private double expectedBytePerSecond;

    private AccumulatorCollector accumulatorCollector;

    private ScheduledExecutorService scheduledExecutorService;

    public ByteRateLimiter(AccumulatorCollector accumulatorCollector, double expectedBytePerSecond) {
        double initialRate = 1000.0;
        this.rateLimiter = RateLimiter.create(initialRate);
        this.expectedBytePerSecond = expectedBytePerSecond;
        this.accumulatorCollector = accumulatorCollector;

        ThreadFactory threadFactory = new BasicThreadFactory
                .Builder()
                .namingPattern("ByteRateCheckerThread-%d")
                .daemon(true)
                .build();
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1, threadFactory);
    }

    public void start(){
        scheduledExecutorService.scheduleAtFixedRate(this::updateRate,0, 1000L, TimeUnit.MILLISECONDS);
    }

    public void stop(){
        if(scheduledExecutorService != null && !scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
    }

    public void acquire() {
        rateLimiter.acquire();
    }

    private void updateRate(){
        long totalBytes = accumulatorCollector.getAccumulatorValue(Metrics.READ_BYTES);
        long thisRecords = accumulatorCollector.getLocalAccumulatorValue(Metrics.NUM_READS);
        long totalRecords = accumulatorCollector.getAccumulatorValue(Metrics.NUM_READS);

        double thisWriteRatio = (totalRecords == 0 ? 0 : thisRecords / (double)totalRecords);

        if (totalRecords > MIN_RECORD_NUMBER_UPDATE_RATE && totalBytes != 0 && thisWriteRatio != 0) {
            double bpr = totalBytes / totalRecords;
            double permitsPerSecond = expectedBytePerSecond / bpr * thisWriteRatio;
            rateLimiter.setRate(permitsPerSecond);
        }
    }
}
