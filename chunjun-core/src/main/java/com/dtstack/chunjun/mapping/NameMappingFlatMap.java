/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dtstack.chunjun.mapping;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * 根据配置中的映射关系将RowData中的表名等信息映射到sink端
 *
 * @author shitou
 * @date 2021/12/14
 */
public class NameMappingFlatMap extends RichFlatMapFunction<RowData, RowData> {

    private final MappingClient client;

    public NameMappingFlatMap(NameMappingConf conf) {
        this.client = new MappingClient(conf);
    }

    @Override
    public void flatMap(RowData value, Collector<RowData> collector) throws Exception {
        RowData rowData = client.map(value);
        collector.collect(rowData);
    }
}
