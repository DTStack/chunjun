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

package com.dtstack.chunjun.connector.iceberg.source;

import com.dtstack.chunjun.connector.iceberg.config.IcebergReaderConfig;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import lombok.NoArgsConstructor;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.flink.source.FlinkInputSplit;

import java.io.IOException;

@NoArgsConstructor
public class IcebergInputFormat extends BaseRichInputFormat {
    private static final long serialVersionUID = 151014153208168637L;

    private FlinkInputFormat flinkInputFormat;
    private IcebergReaderConfig icebergReaderConfig;
    private StreamExecutionEnvironment env;

    public void setInput(FlinkInputFormat input) {
        this.flinkInputFormat = input;
    }

    public void setIcebergReaderConf(IcebergReaderConfig icebergReaderConfig) {
        this.icebergReaderConfig = icebergReaderConfig;
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        return flinkInputFormat.createInputSplits(minNumSplits);
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        flinkInputFormat.open((FlinkInputSplit) inputSplit);
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        RowData genericRowData = flinkInputFormat.nextRecord(rowData);
        try {
            RowData columnRowData = rowConverter.toInternal(genericRowData);
            return columnRowData;
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
    }

    @Override
    protected void closeInternal() throws IOException {
        flinkInputFormat.close();
    }

    @Override
    public boolean reachedEnd() {
        return flinkInputFormat.reachedEnd();
    }
}
