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
package com.dtstack.chunjun.config;

import java.io.Serializable;
import java.util.StringJoiner;

public class SpeedConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 任务全局并行度 */
    private int channel = 1;
    /** source并行度，-1代表采用全局并行度 */
    private int readerChannel = -1;
    /** sink并行度，-1代表采用全局并行度 */
    private int writerChannel = -1;
    /** 速率上限，0代表不限速 */
    private long bytes = 0;
    /** 是否强制进行rebalance，开启会消耗性能 */
    private boolean rebalance = false;

    public int getChannel() {
        return channel;
    }

    public void setChannel(int channel) {
        this.channel = channel;
    }

    public int getReaderChannel() {
        return readerChannel;
    }

    public void setReaderChannel(int readerChannel) {
        this.readerChannel = readerChannel;
    }

    public int getWriterChannel() {
        return writerChannel;
    }

    public void setWriterChannel(int writerChannel) {
        this.writerChannel = writerChannel;
    }

    public long getBytes() {
        return bytes;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public boolean isRebalance() {
        return rebalance;
    }

    public void setRebalance(boolean rebalance) {
        this.rebalance = rebalance;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SpeedConfig.class.getSimpleName() + "[", "]")
                .add("channel=" + channel)
                .add("readerChannel=" + readerChannel)
                .add("writerChannel=" + writerChannel)
                .add("bytes=" + bytes)
                .add("rebalance=" + rebalance)
                .toString();
    }
}
