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

package com.dtstack.chunjun.connector.inceptor.sink;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import org.apache.commons.collections.MapUtils;

/** @author liuliu 2022/2/24 */
public class InceptorSinkFactory extends SinkFactory {

    SinkFactory sinkFactory;

    public InceptorSinkFactory(SyncConf syncConf) {
        super(syncConf);
        boolean useJdbc = !syncConf.getWriter().getParameter().containsKey("path");
        boolean transaction =
                MapUtils.getBoolean(syncConf.getWriter().getParameter(), "isTransaction", false);

        if (useJdbc || transaction) {
            this.sinkFactory = new InceptorJdbcSinkFactory(syncConf);
        } else {
            this.sinkFactory = new InceptorFileSinkFactory(syncConf);
        }
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        return sinkFactory.createSink(dataSet);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return sinkFactory.getRawTypeConverter();
    }
}
