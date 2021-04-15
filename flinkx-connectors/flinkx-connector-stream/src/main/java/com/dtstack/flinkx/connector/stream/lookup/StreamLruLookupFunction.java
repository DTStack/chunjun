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

package com.dtstack.flinkx.connector.stream.lookup;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.dtstack.flinkx.connector.stream.conf.StreamLookupConf;
import com.dtstack.flinkx.lookup.AbstractLruTableFunction;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author chuixue
 * @create 2021-04-09 09:40
 * @description 异步lru维表
 **/
public class StreamLruLookupFunction extends AbstractLruTableFunction {

    public StreamLruLookupFunction(
            StreamLookupConf lookupOptions,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames) {
        super(lookupOptions, null);
    }

    @Override
    public void handleAsyncInvoke(
            CompletableFuture<Collection<RowData>> future,
            Object... keys) throws Exception {
        System.out.println(keys);
    }

    @Override
    protected RowData fillData(
            Object sideInput) {
        return null;
    }
}
