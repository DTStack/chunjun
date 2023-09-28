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

package com.dtstack.chunjun.dirty.consumer;

import com.dtstack.chunjun.dirty.DirtyConfig;
import com.dtstack.chunjun.dirty.impl.DirtyDataEntry;
import com.dtstack.chunjun.throwable.NoRestartException;

import org.apache.flink.api.common.accumulators.LongCounter;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.StringJoiner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.dtstack.chunjun.dirty.utils.LogUtil.warn;

@Slf4j
public abstract class DirtyDataCollector implements Runnable, Serializable {

    protected final LongCounter failedConsumedCounter = new LongCounter(0L);

    protected final LongCounter consumedCounter = new LongCounter(0L);

    private static final long serialVersionUID = 1L;

    /** private dirty data every ${printRate} */
    protected long printRate = Long.MAX_VALUE;

    /**
     * This is the limit on the max consumed-data. The consumer would to be killed with throwing a
     * {@link NoRestartException} when the consumed-count exceed the limit. The default is 1, which
     * means task fails once dirty data occurs.
     */
    protected long maxConsumed = 1L;

    /**
     * This is the limit on the max failed-consumed-data. Same as {@link
     * DirtyDataCollector#maxConsumed}
     */
    protected long maxFailedConsumed = 1L;

    /** The flag of consumer thread. */
    protected AtomicBoolean isRunning = new AtomicBoolean(true);

    /** The queue stored the data not yet consumed. */
    protected LinkedBlockingQueue<DirtyDataEntry> consumeQueue = new LinkedBlockingQueue<>();

    /**
     * Offer data into the blocking-queue.
     *
     * @param dirty dirty data.
     */
    public synchronized void offer(DirtyDataEntry dirty, long globalErrors) {
        consumeQueue.offer(dirty);
        addConsumed(1L, dirty, globalErrors);
    }

    public void initializeConsumer(DirtyConfig conf) {
        this.maxConsumed = conf.getMaxConsumed();
        this.maxFailedConsumed = conf.getMaxFailedConsumed();

        this.init(conf);
    }

    @Override
    public void run() {
        while (isRunning.get()) {
            try {
                DirtyDataEntry dirty = consumeQueue.take();
                consume(dirty);
            } catch (Exception e) {
                addFailedConsumed(e, 1L);
            }
        }
    }

    protected void addConsumed(long count, DirtyDataEntry dirty, long globalErrors) {
        consumedCounter.add(count);
        // 因为总体的脏数据需要tm和jm进行通讯（每tm心跳+1s），会有延迟，且当单slot运行时误差将达到最大
        // 所以这里需要判断延迟情况
        long max =
                consumedCounter.getLocalValue() >= globalErrors
                        ? consumedCounter.getLocalValue()
                        : globalErrors;
        // 但这里仍然有误差：此时如果所有的slot都消费了脏数据那么其他slot的脏数据就记录不到。也就是会多消费脏数据
        // 所以这里要有取舍：是否要消费完全准确的脏数据
        if (max >= maxConsumed) {
            StringJoiner dirtyMessage =
                    new StringJoiner("\n")
                            .add("\n****************Dirty Data Begin****************\n")
                            .add(dirty.toString())
                            .add("\n****************Dirty Data End******************\n");
            throw new NoRestartException(
                    String.format(
                                    "The dirty consumer shutdown, due to the consumed count exceed the max-consumed [%s]",
                                    maxConsumed)
                            + dirtyMessage);
        }
    }

    protected void addFailedConsumed(Throwable cause, long failedCount) {
        failedConsumedCounter.add(failedCount);
        warn(
                log,
                "dirty-plugins consume failed.",
                cause,
                printRate,
                failedConsumedCounter.getLocalValue());

        if (failedConsumedCounter.getLocalValue() >= maxFailedConsumed) {
            throw new NoRestartException(
                    String.format(
                            "The dirty consumer shutdown, due to the failed-consumed count exceed the max-failed-consumed [%s]",
                            maxFailedConsumed));
        }
    }

    public LongCounter getConsumed() {
        return consumedCounter;
    }

    public LongCounter getFailedConsumed() {
        return failedConsumedCounter;
    }

    public void open() {}

    /**
     * Initialize the consumer with {@link DirtyConfig}
     *
     * @param conf dirty conf.
     */
    protected abstract void init(DirtyConfig conf);

    /**
     * Consume the dirty data.
     *
     * @param dirty dirty-data which should be consumed.
     * @throws Exception exception.
     */
    protected abstract void consume(DirtyDataEntry dirty) throws Exception;

    /** Close and release resource, and flush the data which is not been consumed in the queue; */
    public abstract void close();
}
