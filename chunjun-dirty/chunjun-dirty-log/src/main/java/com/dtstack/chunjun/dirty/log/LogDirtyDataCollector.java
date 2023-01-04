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

package com.dtstack.chunjun.dirty.log;

import com.dtstack.chunjun.dirty.DirtyConfig;
import com.dtstack.chunjun.dirty.consumer.DirtyDataCollector;
import com.dtstack.chunjun.dirty.impl.DirtyDataEntry;
import lombok.extern.slf4j.Slf4j;

import java.util.StringJoiner;

@Slf4j
public class LogDirtyDataCollector extends DirtyDataCollector {

    private static final long serialVersionUID = 7366317208451727471L;
    private Long printRate;

    @Override
    protected void init(DirtyConfig conf) {
        this.printRate = conf.getPrintRate();
    }

    @Override
    protected void consume(DirtyDataEntry dirty) {
        if (consumedCounter.getLocalValue() % printRate == 0) {
            StringJoiner dirtyMessage =
                    new StringJoiner("\n")
                            .add("\n====================Dirty Data=====================")
                            .add(dirty.toString())
                            .add("\n===================================================");
            log.warn(dirtyMessage.toString());
        }
    }

    @Override
    public void close() {
        isRunning.compareAndSet(true, false);
        log.info("Print consumer closed.");
    }
}
