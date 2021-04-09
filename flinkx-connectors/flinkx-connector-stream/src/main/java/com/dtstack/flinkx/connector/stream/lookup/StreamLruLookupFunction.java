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
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author chuixue
 * @create 2021-04-09 09:40
 * @description 异步lru维表
 **/
public class StreamLruLookupFunction extends AsyncTableFunction<RowData> {
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    public void eval(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
    }

    @Override
    public void close() throws Exception {
    }
}
