/*
 *    Copyright 2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.dtstack.flinkx.connector.pgwal.inputformat;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import com.dtstack.flinkx.connector.api.SimpleInputSplit;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class DebeziumPGWalInputFormat extends BaseRichInputFormat implements DebeziumDeserializationSchema<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumPGWalInputFormat.class);

    private DebeziumDeserializationSchema<RowData> delegated;
    private QueueCollector queueCollector;

    public DebeziumPGWalInputFormat(RowType rowType,
                                    TypeInformation<RowData> resultTypeInfo,
                                    RowDataDebeziumDeserializeSchema.ValueValidator validator,
                                    ZoneId serverTimeZone,
                                    int size) {
        delegated = new RowDataDebeziumDeserializeSchema(rowType, resultTypeInfo, validator, serverTimeZone);
        queueCollector = new QueueCollector(new ArrayBlockingQueue<>(size));
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        return new InputSplit[]{new SimpleInputSplit(minNumSplits)};
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {

    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws IOException {
        return queueCollector.queue.peek();
    }

    @Override
    protected void closeInternal() throws IOException {
        LOG.info("close internal");
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<RowData> collector) throws Exception {
        delegated.deserialize(sourceRecord, queueCollector);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return delegated.getProducedType();
    }

    static class QueueCollector implements Collector<RowData> {
        private final BlockingQueue<RowData> queue;

        QueueCollector(BlockingQueue<RowData> queue) {
            this.queue = queue;
        }

        @Override
        public void collect(RowData record) {
            this.queue.add(record);
        }

        @Override
        public void close() {
            queue.clear();
        }
    }
}
