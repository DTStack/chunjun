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

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.DdlRowDataConvented;
import com.dtstack.chunjun.cdc.ddl.ConventException;
import com.dtstack.chunjun.cdc.ddl.ConventExceptionProcessHandler;
import com.dtstack.chunjun.cdc.ddl.DdlConvent;
import com.dtstack.chunjun.cdc.ddl.entity.DdlData;
import com.dtstack.chunjun.element.ColumnRowData;

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
    private final DdlConvent source;
    private final DdlConvent sink;
    private final ConventExceptionProcessHandler conventExceptionProcessHandler;

    public NameMappingFlatMap(
            NameMappingConf conf,
            DdlConvent source,
            DdlConvent sink,
            ConventExceptionProcessHandler conventExceptionProcessHandler) {
        this.client = new MappingClient(conf);
        this.source = source;
        this.sink = sink;
        this.conventExceptionProcessHandler = conventExceptionProcessHandler;
        check(source, sink, conventExceptionProcessHandler);
    }

    @Override
    public void flatMap(RowData value, Collector<RowData> collector) {
        RowData rowData = client.map(value);
        if (rowData instanceof ColumnRowData) {
            collector.collect(rowData);
        } else if (rowData instanceof DdlRowData) {
            if (source != null) {
                try {
                    DdlData data = source.rowConventToDdlData((DdlRowData) rowData);
                    data = client.map(data);
                    String s = sink.ddlDataConventToSql(data);
                    ((DdlRowData) rowData).replaceData("content", s);
                    collector.collect(new DdlRowDataConvented((DdlRowData) rowData, null));
                } catch (ConventException e) {
                    conventExceptionProcessHandler.process((DdlRowData) rowData, e, collector);
                }
            } else {
                // 没有转换 就直接传递下游
                collector.collect(value);
            }
        }
    }

    private void check(DdlConvent source, DdlConvent sink, ConventExceptionProcessHandler handler) {
        if (source != null || sink != null || handler != null) {
            if (source != null && sink != null && handler != null) {
                return;
            }
            StringBuilder s = new StringBuilder();
            if (source == null) {
                s.append("source convent not allow null");
            }
            if (sink == null) {
                s.append("sink convent not allow null");
            }

            if (handler == null) {
                s.append("handler not allow null");
            }
            throw new IllegalArgumentException(s.toString());
        }
    }
}
