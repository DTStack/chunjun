/**
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

/**
 * This class is user for speed control
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ByteRateLimiter {

    private RateLimiter rateLimiter;

    private double expectedBytePerSecond;

    private AccumulatorCollector accumulatorCollector;

    private long lastThisWriteRecords;

    private long lastTotalWriteRecords;

    public ByteRateLimiter(AccumulatorCollector accumulatorCollector, double expectedBytePerSecond) {
        double initialRate = 1000.0;
        this.rateLimiter = RateLimiter.create(initialRate);
        this.expectedBytePerSecond = expectedBytePerSecond;
        this.accumulatorCollector = accumulatorCollector;
    }

    public void acquire() {
        updateRate();
        rateLimiter.acquire();
    }

    private void updateRate(){
        long totalWriteBytes = accumulatorCollector.getAccumulatorValue(Metrics.WRITE_BYTES);
        long thisWriteRecords = accumulatorCollector.getLocalAccumulatorValue(Metrics.NUM_WRITES);
        long totalWriteRecords = accumulatorCollector.getAccumulatorValue(Metrics.NUM_WRITES);

        if(lastTotalWriteRecords == totalWriteRecords && lastThisWriteRecords == thisWriteRecords){
            return;
        }

        double thisWriteRatio = (totalWriteRecords == 0 ? 0 : thisWriteRecords / totalWriteRecords);

        if (totalWriteRecords > 1000 && totalWriteBytes != 0 && thisWriteRatio != 0) {
            double bpr = totalWriteBytes / totalWriteRecords;
            double permitsPerSecond = expectedBytePerSecond / bpr * thisWriteRatio;
            rateLimiter.setRate(permitsPerSecond);
        }

        lastThisWriteRecords = thisWriteRecords;
        lastTotalWriteRecords = totalWriteRecords;
    }
}
