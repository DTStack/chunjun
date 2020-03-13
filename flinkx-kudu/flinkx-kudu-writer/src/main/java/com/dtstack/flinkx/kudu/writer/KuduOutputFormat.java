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
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import org.apache.kudu.client.*;

import java.io.IOException;
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

    private void writeData(Row row) throws WriteRecordException {
        int index = 0;
        try {
            Operation operation = getOperation();
            for (int i = 0; i < columns.size(); i++) {
                index = i;
                MetaColumn column = columns.get(i);
                operation.getRow().addObject(column.getName(), row.getField(i));
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
