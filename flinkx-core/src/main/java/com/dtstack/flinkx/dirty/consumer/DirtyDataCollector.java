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

package com.dtstack.flinkx.dirty.consumer;

import com.dtstack.flinkx.dirty.DirtyConf;
import com.dtstack.flinkx.dirty.impl.DirtyDataEntry;
import com.dtstack.flinkx.throwable.NoRestartException;

import org.apache.flink.api.common.accumulators.LongCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.dtstack.flinkx.dirty.utils.LogUtil.warn;

/**
 * @author tiezhu@dtstack
 * @date 22/09/2021 Wednesday
 */
public abstract class DirtyDataCollector implements Runnable, Serializable {

    protected static final LongCounter FAILED_CONSUMED_COUNTER = new LongCounter(0L);

    protected static final LongCounter CONSUMED_COUNTER = new LongCounter(0L);

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DirtyDataCollector.class);

    /** private dirty data every ${printRate} */
    protected long printRate = Long.MAX_VALUE;

    /**
     * This is the limit on the max consumed-data. The consumer would to be killed with throwing a
     * {@link NoRestartException} when the consumed-count exceed the limit. The default is 1, which
     * means task fails once dirty data occurs..
     */
    protected long maxConsumed = 1L;

    /**
     * This is the limit on the max failed-consumed-data. Same as {@link
     * DirtyDataCollector#maxConsumed}
     */
    protected long maxFailedConsumed = 1L;

    /** The flag of consumer thread. */
    protected AtomicBoolean isRunning = new AtomicBoolean(true);

    /** The counter which count the number of consumed-dirty-data. */
    protected transient ThreadLocal<LongCounter> consumed;

    /** The counter which count the number of failed-consumed-dirty-data. */
    protected transient ThreadLocal<LongCounter> failedConsumed;

    /** The queue stored the data not yet consumed. */
    protected LinkedBlockingQueue<DirtyDataEntry> consumeQueue = new LinkedBlockingQueue<>();

    /**
     * Offer data into the blocking-queue.
     *
     * @param dirty dirty data.
     */
    public synchronized void offer(DirtyDataEntry dirty) {
        consumeQueue.offer(dirty);
        addConsumed(1L);
    }

    public void initializeConsumer(DirtyConf conf) {
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
            } finally {
                consumed.remove();
                failedConsumed.remove();
            }
        }
    }

    protected void addConsumed(long count) {
        try {
            CONSUMED_COUNTER.add(count);
            consumed.set(CONSUMED_COUNTER);
            if (consumed.get().getLocalValue() >= maxConsumed) {
                throw new NoRestartException(
                        String.format(
                                "The dirty consumer shutdown, due to the consumed count exceed the max-consumed [%s]",
                                maxConsumed));
            }
        } finally {
            consumed.remove();
        }
    }

    protected void addFailedConsumed(Throwable cause, long failedCount) {
        try {
            FAILED_CONSUMED_COUNTER.add(failedCount);
            failedConsumed.set(CONSUMED_COUNTER);
            warn(
                    LOG,
                    "dirty-plugins consume failed.",
                    cause,
                    printRate,
                    failedConsumed.get().getLocalValue());

            if (failedConsumed.get().getLocalValue() >= maxFailedConsumed) {
                throw new NoRestartException(
                        String.format(
                                "The dirty consumer shutdown, due to the failed-consumed count exceed the max-failed-consumed [%s]",
                                maxFailedConsumed));
            }
        } finally {
            failedConsumed.remove();
        }
    }

    public LongCounter getConsumed() {
        if (consumed == null) {
            consumed = new ThreadLocal<>();
            consumed.set(CONSUMED_COUNTER);
        }
        return CONSUMED_COUNTER;
    }

    public LongCounter getFailedConsumed() {
        if (failedConsumed == null) {
            failedConsumed = new ThreadLocal<>();
            failedConsumed.set(FAILED_CONSUMED_COUNTER);
        }
        return FAILED_CONSUMED_COUNTER;
    }

    public void open() {}

    /**
     * Initialize the consumer with {@link DirtyConf}
     *
     * @param conf dirty conf.
     */
    protected abstract void init(DirtyConf conf);

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
