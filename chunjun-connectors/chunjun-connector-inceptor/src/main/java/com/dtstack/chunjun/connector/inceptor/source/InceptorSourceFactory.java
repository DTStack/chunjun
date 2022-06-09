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

package com.dtstack.chunjun.connector.inceptor.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import org.apache.commons.collections.MapUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** @author liuliu 2022/2/22 */
public class InceptorSourceFactory extends SourceFactory {
    private SourceFactory sourceFactory;

    public InceptorSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        boolean useJdbc = !syncConf.getReader().getParameter().containsKey("path");

        boolean transaction =
                MapUtils.getBoolean(syncConf.getReader().getParameter(), "isTransaction", false);
        // 事务表直接jdbc读取
        if (useJdbc || transaction) {
            refactorConf(syncConf);
            this.sourceFactory = new InceptorJdbcSourceFactory(syncConf, env);
        } else {
            this.sourceFactory = new InceptorFileSourceFactory(syncConf, env);
        }
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return sourceFactory.getRawTypeConverter();
    }

    @Override
    public DataStream<RowData> createSource() {
        return sourceFactory.createSource();
    }

    private void refactorConf(SyncConf syncConf) {
        if (syncConf.getReader().getParameter().containsKey("connection")) {
            Object connection = syncConf.getReader().getParameter().get("connection");
            if (connection instanceof List) {
                List<Map<String, Object>> connections = (List<Map<String, Object>>) connection;
                connections.forEach(
                        i -> {
                            if (i.get("jdbcUrl") != null && i.get("jdbcUrl") instanceof String) {
                                i.put("jdbcUrl", Collections.singletonList(i.get("jdbcUrl")));
                            }
                        });
            }
        }
    }
}
