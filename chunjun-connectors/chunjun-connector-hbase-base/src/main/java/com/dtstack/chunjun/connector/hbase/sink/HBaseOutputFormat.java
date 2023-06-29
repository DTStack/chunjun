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

package com.dtstack.chunjun.connector.hbase.sink;

import com.dtstack.chunjun.connector.hbase.util.HBaseHelper;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** The Hbase Implementation of OutputFormat */
@Slf4j
public class HBaseOutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = 7187047918877677797L;

    private Map<String, Object> hbaseConfig;

    private String tableName;
    private long writeBufferSize;

    private transient Connection connection;
    private transient BufferedMutator bufferedMutator;
    private transient Table table;

    @Override
    public void configure(Configuration parameters) {}

    @Override
    protected void writeSingleRecordInternal(RowData rawRecord) throws WriteRecordException {
        int i = 0;
        try {
            Mutation mutation = null;
            mutation = (Mutation) rowConverter.toExternal(rawRecord, mutation);
            bufferedMutator.mutate(mutation);
            bufferedMutator.flush();
        } catch (Exception ex) {
            if (i < rawRecord.getArity()) {
                throw new WriteRecordException(
                        recordConvertDetailErrorMessage(i, rawRecord), ex, i, rawRecord);
            }
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }

    @Override
    public void openInternal(int taskNumber, int numTasks) throws IOException {
        openConnection();
    }

    public void openConnection() {
        log.info("HbaseOutputFormat configure start");
        Validate.isTrue(hbaseConfig != null && hbaseConfig.size() != 0, "hbaseConfig不能为空Map结构!");

        try {
            connection =
                    HBaseHelper.getHbaseConnection(hbaseConfig, jobId, String.valueOf(taskNumber));
            org.apache.hadoop.conf.Configuration hConfiguration =
                    HBaseHelper.getConfig(hbaseConfig);
            try (Admin admin = this.connection.getAdmin()) {
                boolean exist = admin.tableExists(TableName.valueOf(tableName));
                if (!exist) {
                    throw new IOException(
                            "Target table is not exist,please check for table: " + tableName);
                }
            }

            bufferedMutator =
                    connection.getBufferedMutator(
                            new BufferedMutatorParams(TableName.valueOf(tableName))
                                    .pool(HTable.getDefaultExecutor(hConfiguration))
                                    .writeBufferSize(writeBufferSize));
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            HBaseHelper.closeBufferedMutator(bufferedMutator);
            HBaseHelper.closeConnection(connection);
            throw new IllegalArgumentException(e);
        }

        log.info("HbaseOutputFormat configure end");
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        Object[] results = new Object[0];
        try {
            List<Mutation> mutations = new ArrayList<>();
            for (RowData record : rows) {
                Mutation mutation = null;
                mutation = (Mutation) rowConverter.toExternal(record, mutation);
                mutations.add(mutation);
            }
            results = new Object[mutations.size()];
            table.batch(mutations, results);
        } catch (IOException e) {
            throw new IOException(e);
        } finally {
            for (int i = 0; i < Objects.requireNonNull(results).length; i++) {
                if (results[i] instanceof Exception) {
                    Exception exception = (Exception) results[i];
                    log.error(exception.getMessage(), exception);
                }
            }
        }
    }

    @Override
    public void closeInternal() throws IOException {
        HBaseHelper.closeBufferedMutator(bufferedMutator);
        HBaseHelper.closeConnection(connection);
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setHbaseConfig(Map<String, Object> hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
    }

    public void setWriteBufferSize(Long writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, Object> getHbaseConfig() {
        return hbaseConfig;
    }
}
