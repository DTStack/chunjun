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

package com.dtstack.flinkx.connector.stream.sink;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.streaming.api.functions.sink.DtOutputFormatSinkFunction;

/**
 * @author chuixue
 * @create 2021-04-08 20:27
 * @description 包含数据的处理逻辑、数据的cp逻辑都抽到父类中，如果父类满足所有需求，则没必要写该类
 **/
public class StreamSinkFunction extends DtOutputFormatSinkFunction<RowData> {
    private static final long serialVersionUID = 1L;

    public StreamSinkFunction(OutputFormat<RowData> format) {
        super(format);
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        format.writeRecord(value);
    }
}
