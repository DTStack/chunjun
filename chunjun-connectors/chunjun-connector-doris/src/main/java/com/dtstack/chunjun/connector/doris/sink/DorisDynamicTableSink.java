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

package com.dtstack.chunjun.connector.doris.sink;

import com.dtstack.chunjun.connector.doris.converter.DorisRowConvert;
import com.dtstack.chunjun.connector.doris.options.DorisConf;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-11-21
 */
public class DorisDynamicTableSink implements DynamicTableSink {

    private final TableSchema physicalSchema;

    private final DorisConf dorisConf;

    public DorisDynamicTableSink(TableSchema physicalSchema, DorisConf dorisConf) {
        this.physicalSchema = physicalSchema;
        this.dorisConf = dorisConf;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        DorisOutputFormatBuilder builder = new DorisOutputFormatBuilder();
        builder.setRowConverter(new DorisRowConvert(rowType));
        builder.setDorisOptions(dorisConf);

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), dorisConf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new DorisDynamicTableSink(physicalSchema, dorisConf);
    }

    @Override
    public String asSummaryString() {
        return "doris sink";
    }
}
