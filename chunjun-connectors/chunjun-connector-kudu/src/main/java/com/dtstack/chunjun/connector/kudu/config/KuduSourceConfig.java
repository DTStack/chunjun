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

import org.apache.flink.configuration.ReadableConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.FILTER_EXPRESSION;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.READ_MODE;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.SCAN_BATCH_SIZE_BYTES;

@EqualsAndHashCode(callSuper = true)
@Data
public class KuduSourceConfig extends KuduCommonConfig {

    private static final long serialVersionUID = -7861666044144669286L;
    /**
     * kudu读取模式： 1、READ_LATEST 默认的读取模式 该模式下，服务器将始终在收到请求时返回已提交的写操作。这种类型的读取不会返回快照时间戳，并且不可重复。
     * 用ACID术语表示，它对应于隔离模式：“读已提交”
     *
     * <p>2、READ_AT_SNAPSHOT 该模式下，服务器将尝试在提供的时间戳上执行读取。如果未提供时间戳，则服务器将当前时间作为快照时间戳。
     * 在这种模式下，读取是可重复的，即将来所有在相同时间戳记下的读取将产生相同的数据。
     * 执行此操作的代价是等待时间戳小于快照的时间戳的正在进行的正在进行的事务，因此可能会导致延迟损失。用ACID术语，这本身就相当于隔离模式“可重复读取”。
     * 如果对已扫描tablet的所有写入均在外部保持一致，则这对应于隔离模式“严格可序列化”。
     * 注意：当前存在“空洞”，在罕见的边缘条件下会发生，通过这种空洞有时即使在采取措施使写入如此时，它们在外部也不一致。 在这些情况下，隔离可能会退化为“读取已提交”模式。
     * 3、READ_YOUR_WRITES 不支持该模式
     */
    private String readMode;

    private int batchSizeBytes = 1024;

    private String filter;

    public static KuduSourceConfig from(ReadableConfig readableConfig) {

        KuduSourceConfig config =
                (KuduSourceConfig) KuduCommonConfig.from(readableConfig, new KuduSourceConfig());

        // source
        config.setReadMode(readableConfig.get(READ_MODE));
        config.setBatchSizeBytes(readableConfig.get(SCAN_BATCH_SIZE_BYTES));
        config.setFilter(readableConfig.get(FILTER_EXPRESSION));

        return config;
    }
}
