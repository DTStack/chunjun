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
package com.dtstack.flinkx.connector.binlog.listener;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.dtstack.flinkx.connector.binlog.inputformat.BinlogInputFormat;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.ErrorMsgRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.MapColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.SnowflakeIdWorker;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author toutian
 */
public class BinlogEventSink extends AbstractCanalLifeCycle implements com.alibaba.otter.canal.sink.CanalEventSink<List<CanalEntry.Entry>> {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogEventSink.class);

    private final BinlogInputFormat format;
    private final LinkedBlockingDeque<RowData> queue;
    private final boolean pavingData;
    private final boolean splitUpdate;
    private final SnowflakeIdWorker idWorker;

    public BinlogEventSink(BinlogInputFormat format) {
        this.format = format;
        this.pavingData = format.getBinlogConf().isPavingData();
        this.splitUpdate = format.getBinlogConf().isSplitUpdate();
        this.queue = new LinkedBlockingDeque<>();
        this.idWorker = new SnowflakeIdWorker(1, 1);
    }

    @Override
    public boolean sink(List<CanalEntry.Entry> entries, InetSocketAddress inetSocketAddress, String s) throws CanalSinkException, InterruptedException {
        for (CanalEntry.Entry entry : entries) {
            CanalEntry.EntryType entryType = entry.getEntryType();
            if (entryType != CanalEntry.EntryType.ROWDATA) {
                continue;
            }
            CanalEntry.RowChange rowChange = null;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                LOG.error("ERROR ## parser of eromanga-event has an error , data: {}", entry);
            }

            if(rowChange == null) {
                return false;
            }

            CanalEntry.Header header = entry.getHeader();
            String schema = header.getSchemaName();
            String table = header.getTableName();
            long executeTime = header.getExecuteTime();
            processRowChange(rowChange, schema, table, executeTime);
        }
        return true;
    }

    /**
     * 处理RowData数据
     * @param rowChange 解析后的RowData数据
     * @param schema  schema
     * @param table table
     * @param executeTime   变更数据的执行时间
     */
    private void processRowChange(CanalEntry.RowChange rowChange, String schema, String table, long executeTime)throws InterruptedException {
        String eventType = rowChange.getEventType().toString();
        List<String> categories = format.getCategories();
        if(CollectionUtils.isNotEmpty(categories) && !categories.contains(eventType)) {
            return;
        }

        for(CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            int size;
            if(pavingData){
                //5: type, schema, table, ts, opTime
                size = 5 + rowData.getAfterColumnsList().size() + rowData.getBeforeColumnsList().size();
            }else{
                //7: type, schema, table, ts, opTime, before, after
                size = 7;
            }
            ColumnRowData columnRowData = new ColumnRowData(size);
            columnRowData.addField(new StringColumn(schema));
            columnRowData.addHeader("schema");
            columnRowData.addField(new StringColumn(table));
            columnRowData.addHeader("table");
            columnRowData.addField(new BigDecimalColumn(idWorker.nextId()));
            columnRowData.addHeader("ts");
            columnRowData.addField(new TimestampColumn(executeTime));
            columnRowData.addHeader("opTime");

            List<AbstractBaseColumn> beforeColumnList = new ArrayList<>(rowData.getBeforeColumnsList().size());
            List<String> beforeHeaderList = new ArrayList<>(rowData.getBeforeColumnsList().size());
            List<AbstractBaseColumn> afterColumnList = new ArrayList<>(rowData.getAfterColumnsList().size());
            List<String> afterHeaderList = new ArrayList<>(rowData.getAfterColumnsList().size());

            if (pavingData){
                for (CanalEntry.Column column : rowData.getBeforeColumnsList()){
                    if(!column.getIsNull()){
                        beforeColumnList.add(new StringColumn(column.getValue()));
                        beforeHeaderList.add("before_" + column.getName());
                    }
                }
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    if(!column.getIsNull()){
                        afterColumnList.add(new StringColumn(column.getValue()));
                        afterHeaderList.add("after_" + column.getName());
                    }
                }
            } else {
                beforeColumnList.add(new MapColumn(processColumnList(rowData.getBeforeColumnsList())));
                beforeHeaderList.add("before");
                afterColumnList.add(new MapColumn(processColumnList(rowData.getAfterColumnsList())));
                afterHeaderList.add("after");
            }

            //update类型且要拆分
            if (splitUpdate && CanalEntry.EventType.UPDATE == rowChange.getEventType()) {
                ColumnRowData copy = columnRowData.copy();
                copy.setRowKind(RowKind.UPDATE_BEFORE);
                copy.addField(new StringColumn(RowKind.UPDATE_BEFORE.name()));
                copy.addHeader("type");
                copy.addAllField(beforeColumnList);
                copy.addAllHeader(beforeHeaderList);
                queue.put(copy);

                columnRowData.setRowKind(RowKind.UPDATE_AFTER);
                columnRowData.addField(new StringColumn(RowKind.UPDATE_AFTER.name()));
                columnRowData.addHeader("type");
            }else{
                columnRowData.setRowKind(getRowKindByEventType(rowChange.getEventType()));
                columnRowData.addField(new StringColumn(eventType));
                columnRowData.addHeader("type");
                columnRowData.addAllField(beforeColumnList);
                columnRowData.addAllHeader(beforeHeaderList);
            }
            columnRowData.addAllField(afterColumnList);
            columnRowData.addAllHeader(afterHeaderList);
            queue.put(columnRowData);
        }
    }

    /**
     * 解析CanalEntry中的Column，获取字段名及值
     * @param columnList
     * @return 字段名和值的map集合
     */
    private Map<String, Object> processColumnList(List<CanalEntry.Column> columnList) {
        return columnList.stream()
                .collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
    }

    /**
     * 根据eventType获取RowKind
     * @param eventType
     * @return
     */
    private RowKind getRowKindByEventType(CanalEntry.EventType eventType){
        switch (eventType){
            case INSERT:
            case UPDATE:
                return RowKind.INSERT;
            case DELETE:
                return RowKind.DELETE;
            default:
                throw new RuntimeException("unsupported eventType: " + eventType);
        }
    }

    /**
     * 从队列中获取RowData数据，对于异常情况需要把异常抛出并停止任务
     * @return
     */
    public RowData takeRowDataFromQueue() {
        RowData rowData = null;
        try {
            //最多阻塞100ms
            rowData = queue.poll(100, TimeUnit.MILLISECONDS);
            //@see com.dtstack.flinkx.binlog.listener.HeartBeatController.onFailed 检测到异常之后 会添加key为e的错误数据
            if(rowData instanceof ErrorMsgRowData){
                throw new RuntimeException(rowData.toString());
            }
        } catch (InterruptedException e) {
            LOG.error("takeRowDataFromQueue interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return rowData;
    }

    /**
     * 处理异常数据
     * @param rowData
     */
    public void processErrorMsgRowData(ErrorMsgRowData rowData) {
        try {
            queue.put(rowData);
        } catch (InterruptedException e) {
            LOG.error("takeRowDataFromQueue interrupted event:{} error:{}", rowData, ExceptionUtil.getErrorMessage(e));
        }
    }

    @Override
    public void interrupt() {
        LOG.warn("BinlogEventSink is interrupted");
    }
}
