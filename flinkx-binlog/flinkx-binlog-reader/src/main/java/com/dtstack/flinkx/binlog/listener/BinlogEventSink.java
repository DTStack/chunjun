/*
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
package com.dtstack.flinkx.binlog.listener;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.dtstack.flinkx.binlog.format.BinlogInputFormat;
import com.dtstack.flinkx.log.DtLogger;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.SnowflakeIdWorker;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * @author toutian
 */
public class BinlogEventSink extends AbstractCanalLifeCycle implements com.alibaba.otter.canal.sink.CanalEventSink<List<CanalEntry.Entry>> {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogEventSink.class);

    private BinlogInputFormat format;

    private BlockingQueue<Map<String,Object>> queue;

    private boolean pavingData;

    private SnowflakeIdWorker idWorker;

    public BinlogEventSink(BinlogInputFormat format) {
        this.format = format;
        queue = new SynchronousQueue<>(false);
        idWorker = new SnowflakeIdWorker(1, 1);
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
            String schema = header.getSchemaName();
            String table = header.getTableName();
            processRowChange(rowChange, schema, table);
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

    private void processRowChange(CanalEntry.RowChange rowChange, String schema, String table) {
        CanalEntry.EventType eventType = rowChange.getEventType();

        if(!format.accept(eventType.toString())) {
            return;
        }

        for(CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            Map<String,Object> message = new HashMap<>(8);
            message.put("type", eventType.toString());
            message.put("schema", schema);
            message.put("table", table);
            message.put("ts", idWorker.nextId());

            if (pavingData){
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    if(!column.getIsNull()){
                        message.put("after_" + column.getName(), column.getValue());
                    }
                }
                for (CanalEntry.Column column : rowData.getBeforeColumnsList()){
                    if(!column.getIsNull()){
                        message.put("before_" + column.getName(), column.getValue());
                    }
                }
            } else {
                message.put("before", processColumnList(rowData.getBeforeColumnsList()));
                message.put("after", processColumnList(rowData.getAfterColumnsList()));
                message = Collections.singletonMap("message", message);
            }

            try {
                queue.put(message);
            } catch (InterruptedException e) {
                LOG.error("takeEvent interrupted message:{} error:{}", message, e);
            }
            if(DtLogger.isEnableTrace()){
                LOG.trace("message = {}", GsonUtil.GSON.toJson(message));
            }
        }

    }

    private Map<String,Object> processColumnList(List<CanalEntry.Column> columnList) {
        Map<String,Object> map = new HashMap<>(columnList.size());
        for (CanalEntry.Column column : columnList) {
            if(!column.getIsNull()){
                map.put(column.getName(), column.getValue());
            }
        }
        return map;
    }

    public void setPavingData(boolean pavingData) {
        this.pavingData = pavingData;
    }

    public Row takeEvent() throws IOException {
        Row row = null;
        try {
            Map<String, Object> map = queue.take();
            //@see com.dtstack.flinkx.binlog.listener.HeartBeatController.onFailed 检测到异常之后 会添加key为e的错误数据
            if(map.size() == 1 && map.containsKey("e")){
                throw new RuntimeException((String) map.get("e"));
            }else{
                row = Row.of(map);
            }
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return row;
    }

    public void processEvent(Map<String, Object> event) {
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted event:{} error:{}", event, ExceptionUtil.getErrorMessage(e));
        }
    }

    @Override
    public void interrupt() {
        LOG.info("BinlogEventSink is interrupted");
    }

}
