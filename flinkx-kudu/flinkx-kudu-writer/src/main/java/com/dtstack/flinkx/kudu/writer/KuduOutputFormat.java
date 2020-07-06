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


package com.dtstack.flinkx.kudu.writer;

import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.kudu.core.KuduConfig;
import com.dtstack.flinkx.kudu.core.KuduUtil;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.ValueUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

/**
 * @author jiangbo
 * @date 2019/7/31
 */
public class KuduOutputFormat extends BaseRichOutputFormat {

    protected List<MetaColumn> columns;

    protected KuduConfig kuduConfig;

    protected String writeMode;

    private transient KuduClient client;

    private transient KuduSession session;

    private transient KuduTable kuduTable;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        try{
            client = KuduUtil.getKuduClient(kuduConfig);
        } catch (Exception e){
            throw new RuntimeException("Get KuduClient error", e);
        }

        session = client.newSession();
        session.setMutationBufferSpace(batchInterval);
        kuduTable = client.openTable(kuduConfig.getTable());

        if(StringUtils.isBlank(kuduConfig.getFlushMode())){
            session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        }else {
            switch (kuduConfig.getFlushMode().toLowerCase()) {
                case "auto_flush_background":
                    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
                    break;
                case "manual_flush":
                    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
                    break;
                default:
                    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
                }
            }
        }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        writeData(row);

        if(numWriteCounter.getLocalValue() % batchInterval == 0){
            LOG.info("writeSingleRecordInternal, numWriteCounter = {}", numWriteCounter.getLocalValue());
            try {
                session.flush();
            } catch (KuduException e) {
                throw new RuntimeException("Flush data error", e);
            }
        }
    }

    /**
     * kudu内部采用(Long) val方式进行数据转换，这里先进行优雅转换
     * @param row 传入数据
     * @throws WriteRecordException 写入异常
     */
    private void writeData(Row row) throws WriteRecordException {
        int index = 0;
        try {
            Operation operation = getOperation();
            PartialRow partialRow = operation.getRow();
            for (int i = 0; i < columns.size(); i++) {
                index = i;
                MetaColumn column = columns.get(i);
                int columnIndex = partialRow.getSchema().getColumnIndex(column.getName());
                ColumnSchema col = partialRow.getSchema().getColumnByIndex(columnIndex);
                if (col == null) {
                    throw new IllegalArgumentException("Column name isn't present in the table's schema");
                }
                Object var = null;
                Object beforValue = row.getField(i);
                switch (col.getType()) {
                    case BOOL: var =  ValueUtil.getBoolean(beforValue); break;
                    case INT8: var =  ValueUtil.getByte(beforValue); break;
                    case INT16: var =  ValueUtil.getShort(beforValue); break;
                    case INT32: var =  ValueUtil.getIntegerVal(beforValue); break;
                    case INT64: var = ValueUtil.getLongVal(beforValue); break;
                    case UNIXTIME_MICROS:
                        if (beforValue instanceof Timestamp) {
                            var =  ValueUtil.getTimestampVal(beforValue);
                        } else {
                            var =  ValueUtil.getLongVal(beforValue);
                        }
                        break;
                    case FLOAT: var = ValueUtil.getFloatVal(beforValue); break;
                    case DOUBLE: var =  ValueUtil.getDoubleVal(beforValue); break;
                    case STRING: var =  ValueUtil.getString(beforValue); break;
                    case BINARY: break;
                    case DECIMAL: var =  ValueUtil.getBigDecimal(beforValue); break;
                    default:
                        throw new IllegalArgumentException("Unsupported column type: " + col.getType());
                }
                partialRow.addObject(columnIndex, var);
            }

            session.apply(operation);
        } catch (Exception e){
            LOG.error("Write data error, index = {}, row = {}, e = {}", index, row, ExceptionUtil.getErrorMessage(e));
            throw new WriteRecordException("Write data error", e, index, row);
        }
    }

    private Operation getOperation(){
        if(EWriteMode.INSERT.name().equalsIgnoreCase(writeMode)){
            return kuduTable.newInsert();
        } else if(EWriteMode.UPDATE.name().equalsIgnoreCase(writeMode)){
            return kuduTable.newUpdate();
        } else if(EWriteMode.UPSERT.name().equalsIgnoreCase(writeMode)){
            return kuduTable.newUpsert();
        } else {
            throw new IllegalArgumentException("Not support writeMode:" + writeMode);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        LOG.info("writeRecordInternal, row size = {}", rows.size());
        for (Row row : rows) {
            writeData(row);
        }
        session.flush();
    }

    @Override
    public void closeInternal() throws IOException {
        super.closeInternal();

        if(session != null){
            session.flush();
            session.close();
        }

        if(client != null){
            client.close();
        }
    }
}
