/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.binlog.reader;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;


public class BinlogEventSink extends AbstractCanalLifeCycle implements com.alibaba.otter.canal.sink.CanalEventSink<List<CanalEntry.Entry>> {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogEventSink.class);

    private BinlogInputFormat format;

    private BlockingQueue<Row> queue;

    private boolean pavingData;

    public BinlogEventSink(BinlogInputFormat format) {
        this.format = format;
        queue = new SynchronousQueue<>(false);
    }

    @Override
    public boolean sink(List<CanalEntry.Entry> entries, InetSocketAddress inetSocketAddress, String s) throws CanalSinkException, InterruptedException {

        for (CanalEntry.Entry entry : entries) {
            CanalEntry.EntryType entryType = entry.getEntryType();
            if (entryType != CanalEntry.EntryType.ROWDATA) {
                continue;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("binlog sink, entryType:{}", entry.getEntryType());
            }

            CanalEntry.RowChange rowChange = parseRowChange(entry);

            if(rowChange == null) {
                return false;
            }

            CanalEntry.Header header = entry.getHeader();
            long ts = header.getExecuteTime();
            String schema = header.getSchemaName();
            String table = header.getTableName();
            processRowChange(rowChange, schema, table, ts);
        }

        return true;
    }

    private CanalEntry.RowChange parseRowChange(CanalEntry.Entry entry) {
        CanalEntry.RowChange rowChange = null;
        try {
            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            LOG.error("ERROR ## parser of eromanga-event has an error , data:" + entry.toString());
        }
        return rowChange;
    }

    private void processRowChange(CanalEntry.RowChange rowChange, String schema, String table, long ts) {
        CanalEntry.EventType eventType = rowChange.getEventType();

        if(!format.accept(eventType.toString())) {
            return;
        }

        for(CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            Map<String,Object> message = new HashMap<>();
            message.put("type", eventType.toString());
            message.put("schema", schema);
            message.put("table", table);
            message.put("ts", ts);
            message.put("ingestion", System.nanoTime());

            if (pavingData){
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    message.put("after_" + column.getName(), column.getValue());
                }
                for (CanalEntry.Column column : rowData.getBeforeColumnsList()){
                    message.put("before_" + column.getName(), column.getValue());
                }
            } else {
                message.put("before", processColumnList(rowData.getBeforeColumnsList()));
                message.put("after", processColumnList(rowData.getAfterColumnsList()));
                Map<String,Object> event = new HashMap<>(1);
                event.put("message", message);
                message = event;
            }

            try {
                queue.put(Row.of(message));
            } catch (InterruptedException e) {
                LOG.error("takeEvent interrupted message:{} error:{}", message, e);
            }
        }

    }

    private Map<String,Object> processColumnList(List<CanalEntry.Column> columnList) {
        Map<String,Object> map = new HashMap<>();
        for (CanalEntry.Column column : columnList) {
            map.put(column.getName(), column.getValue());
        }
        return map;
    }

    public void setPavingData(boolean pavingData) {
        this.pavingData = pavingData;
    }

    public Row takeEvent() {
        Row row = null;
        try {
            row = queue.take();
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", e);
        }
        return row;
    }

    @Override
    public void interrupt() {
        LOG.info("BinlogEventSink is interrupted");
    }

}
