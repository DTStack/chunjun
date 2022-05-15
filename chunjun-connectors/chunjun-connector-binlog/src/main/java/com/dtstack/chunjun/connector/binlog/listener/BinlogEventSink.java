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
package com.dtstack.chunjun.connector.binlog.listener;

import com.dtstack.chunjun.connector.binlog.inputformat.BinlogInputFormat;
import com.dtstack.chunjun.converter.AbstractCDCRowConverter;
import com.dtstack.chunjun.element.ErrorMsgRowData;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.table.data.RowData;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/** @author toutian */
public class BinlogEventSink extends AbstractCanalLifeCycle
        implements com.alibaba.otter.canal.sink.CanalEventSink<List<CanalEntry.Entry>> {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogEventSink.class);

    private final BinlogInputFormat format;
    private final LinkedBlockingDeque<RowData> queue;
    private final AbstractCDCRowConverter rowConverter;

    public BinlogEventSink(BinlogInputFormat format) {
        this.format = format;
        this.queue = new LinkedBlockingDeque<>();
        this.rowConverter = format.getRowConverter();
    }

    @Override
    public boolean sink(
            List<CanalEntry.Entry> entries, InetSocketAddress inetSocketAddress, String s)
            throws CanalSinkException {
        for (CanalEntry.Entry entry : entries) {
            CanalEntry.EntryType entryType = entry.getEntryType();
            if (entryType != CanalEntry.EntryType.ROWDATA) {
                continue;
            }
            CanalEntry.RowChange rowChange = null;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                LOG.error("parser data[{}] error:{}", entry, ExceptionUtil.getErrorMessage(e));
            }

            if (rowChange == null) {
                return false;
            }

            CanalEntry.Header header = entry.getHeader();
            String schema = header.getSchemaName();
            String table = header.getTableName();
            long executeTime = header.getExecuteTime();
            try {
                processRowChange(rowChange, schema, table, executeTime);
            } catch (WriteRecordException e) {
                // todo 脏数据记录
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "write error rowData, rowData = {}, e = {}",
                            e.getRowData().toString(),
                            ExceptionUtil.getErrorMessage(e));
                }
            }
        }
        return true;
    }

    /**
     * 处理RowData数据
     *
     * @param rowChange 解析后的RowData数据
     * @param schema schema
     * @param table table
     * @param executeTime 变更数据的执行时间
     */
    @SuppressWarnings("unchecked")
    private void processRowChange(
            CanalEntry.RowChange rowChange, String schema, String table, long executeTime)
            throws WriteRecordException {
        String eventType = rowChange.getEventType().toString();
        List<String> categories = format.getCategories();
        if (CollectionUtils.isNotEmpty(categories) && !categories.contains(eventType)) {
            return;
        }
        BinlogEventRow binlogEventRow = new BinlogEventRow(rowChange, schema, table, executeTime);
        LinkedList<RowData> rowDatalist = null;
        try {
            rowDatalist = rowConverter.toInternal(binlogEventRow);
        } catch (Exception e) {
            throw new WriteRecordException("", e, 0, binlogEventRow);
        }
        RowData rowData = null;
        try {
            while (rowDatalist != null && (rowData = rowDatalist.poll()) != null) {
                queue.put(rowData);
            }
        } catch (InterruptedException e) {
            LOG.error(
                    "put rowData[{}] into queue interrupted error:{}",
                    rowData,
                    ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 从队列中获取RowData数据，对于异常情况需要把异常抛出并停止任务
     *
     * @return
     */
    public RowData takeRowDataFromQueue() {
        RowData rowData = null;
        try {
            // 最多阻塞100ms
            rowData = queue.poll(100, TimeUnit.MILLISECONDS);
            if (rowData instanceof ErrorMsgRowData) {
                throw new RuntimeException(rowData.toString());
            }
        } catch (InterruptedException e) {
            LOG.error(
                    "takeRowDataFromQueue interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return rowData;
    }

    /**
     * 处理异常数据
     *
     * @param rowData
     */
    public void processErrorMsgRowData(ErrorMsgRowData rowData) {
        try {
            queue.put(rowData);
        } catch (InterruptedException e) {
            LOG.error(
                    "processErrorMsgRowData interrupted rowData:{} error:{}",
                    rowData,
                    ExceptionUtil.getErrorMessage(e));
        }
    }

    @Override
    public void interrupt() {
        LOG.warn("BinlogEventSink is interrupted");
    }
}
