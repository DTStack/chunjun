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

package com.dtstack.chunjun.connector.kudu.config;

import com.dtstack.chunjun.sink.WriteMode;

import org.apache.flink.configuration.ReadableConfig;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Locale;

import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.FLUSH_MODE;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.MUTATION_BUFFER_SPACE;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.WRITE_MODE;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.dtstack.chunjun.table.options.SinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;

public class KuduSinkConfig extends KuduCommonConfig {

    /** writer写入时session刷新模式 auto_flush_sync（默认） auto_flush_background manual_flush */
    private String flushMode = "auto_flush_sync";

    private WriteMode writeMode = WriteMode.APPEND;

    private int maxBufferSize = 1024;

    private long flushInterval = 10 * 1000;

    public static KuduSinkConfig from(ReadableConfig readableConfig) {
        KuduSinkConfig conf =
                (KuduSinkConfig) KuduCommonConfig.from(readableConfig, new KuduSinkConfig());

        // sink
        conf.setBatchSize(readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        conf.setWriteMode(readableConfig.get(WRITE_MODE));
        conf.setMaxBufferSize(readableConfig.get(MUTATION_BUFFER_SPACE));
        conf.setFlushMode(readableConfig.get(FLUSH_MODE));
        conf.setFlushInterval(readableConfig.get(SINK_BUFFER_FLUSH_INTERVAL));

        return conf;
    }

    public String getFlushMode() {
        return flushMode;
    }

    public void setFlushMode(String flushMode) {
        this.flushMode = flushMode;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        switch (writeMode.toLowerCase(Locale.ENGLISH)) {
            case "insert":
                this.writeMode = WriteMode.INSERT;
                break;
            case "update":
                this.writeMode = WriteMode.UPDATE;
                break;
            case "upsert":
                this.writeMode = WriteMode.UPSERT;
                break;
            default:
                this.writeMode = WriteMode.APPEND;
        }
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    public long getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(long flushInterval) {
        this.flushInterval = flushInterval;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("masters", masters)
                .append("flush-mode", flushMode)
                .append("flush-interval", flushInterval)
                .append("write-mode", writeMode)
                .toString();
    }
}
