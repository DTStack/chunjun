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

package com.dtstack.flinkx.connector.hbase14.sink;

import com.dtstack.flinkx.connector.hbase.HBaseMutationConverter;
import com.dtstack.flinkx.connector.hbase14.converter.DataSyncSinkConverter;
import com.dtstack.flinkx.connector.hbase14.util.HBaseConfigUtils;
import com.dtstack.flinkx.connector.hbase14.util.HBaseHelper;
import com.dtstack.flinkx.security.KerberosUtil;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.WriteRecordException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.zookeeper.client.ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY;

/**
 * The Hbase Implementation of OutputFormat
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class HBaseOutputFormat extends BaseRichOutputFormat {

    private Map<String, Object> hbaseConfig;

    private String tableName;
    private String encoding;
    private String nullMode;
    private boolean walFlag;
    private long writeBufferSize;

    private List<String> columnTypes;
    private List<String> columnNames;

    private String rowkeyExpress;
    private Integer versionColumnIndex;

    private String versionColumnValue;

    private transient Connection connection;
    private transient BufferedMutator bufferedMutator;

    private transient Table table;

    private HBaseMutationConverter mutationConverter;
    private DataSyncSinkConverter dataSyncSinkConverter;

    public void setMutationConverter(HBaseMutationConverter mutationConverter) {
        this.mutationConverter = mutationConverter;
    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    protected void writeSingleRecordInternal(RowData rawRecord) throws WriteRecordException {
        int i = 0;
        try {
            if (mutationConverter != null) {
                bufferedMutator.mutate(mutationConverter.convertToMutation(rawRecord));
            } else {
                bufferedMutator.mutate(dataSyncSinkConverter.generatePutCommand(rawRecord));
            }
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
        boolean openKerberos = HBaseConfigUtils.isEnableKerberos(hbaseConfig);
        if (openKerberos) {
            // TDH环境并且zk开启了kerberos需要设置zk的环境变量
            if (HBaseHelper.openKerberosForZk(hbaseConfig)) {
                String keytabFile =
                        HBaseHelper.getKeyTabFileName(
                                hbaseConfig, getRuntimeContext().getDistributedCache(), jobId);
                String principal = KerberosUtil.getPrincipal(hbaseConfig, keytabFile);
                String client = System.getProperty(LOGIN_CONTEXT_NAME_KEY, "Client");
                KerberosUtil.appendOrUpdateJaasConf(client, keytabFile, principal);
            }
            UserGroupInformation ugi =
                    HBaseHelper.getUgi(
                            hbaseConfig, getRuntimeContext().getDistributedCache(), jobId);
            ugi.doAs(
                    (PrivilegedAction<Object>)
                            () -> {
                                openConnection();
                                return null;
                            });
        } else {
            openConnection();
        }
        if (mutationConverter != null) {
            mutationConverter.open();
        } else {
            dataSyncSinkConverter =
                    new DataSyncSinkConverter(
                            walFlag,
                            nullMode,
                            encoding,
                            columnTypes,
                            columnNames,
                            rowkeyExpress,
                            versionColumnIndex,
                            versionColumnValue);
        }
    }

    public void openConnection() {
        LOG.info("HbaseOutputFormat configure start");
        Validate.isTrue(hbaseConfig != null && hbaseConfig.size() != 0, "hbaseConfig不能为空Map结构!");

        try {
            org.apache.hadoop.conf.Configuration hConfiguration =
                    HBaseHelper.getConfig(hbaseConfig);
            connection = ConnectionFactory.createConnection(hConfiguration);

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

        LOG.info("HbaseOutputFormat configure end");
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        Object[] results = new Object[0];
        try {
            List<Mutation> mutations = new ArrayList<>();
            for (RowData record : rows) {
                if (mutationConverter != null) {
                    mutations.add(mutationConverter.convertToMutation(record));
                } else {
                    mutations.add(dataSyncSinkConverter.generatePutCommand(record));
                }
            }
            results = new Object[mutations.size()];
            table.batch(mutations, results);
        } catch (IOException e) {
            throw new IOException(e);
        } finally {
            for (int i = 0; i < Objects.requireNonNull(results).length; i++) {
                if (results[i] instanceof Exception) {
                    Exception exception = (Exception) results[i];
                    LOG.error(exception.getMessage(), exception);
                }
            }
        }
    }

    @Override
    public void closeInternal() throws IOException {
        if (dataSyncSinkConverter != null) {
            dataSyncSinkConverter.close();
        }
        HBaseHelper.closeBufferedMutator(bufferedMutator);
        HBaseHelper.closeConnection(connection);
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setHbaseConfig(Map<String, Object> hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
    }

    public void setColumnTypes(List<String> columnTypes) {
        this.columnTypes = columnTypes;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public void setRowkeyExpress(String rowkeyExpress) {
        this.rowkeyExpress = rowkeyExpress;
    }

    public void setVersionColumnIndex(Integer versionColumnIndex) {
        this.versionColumnIndex = versionColumnIndex;
    }

    public void setVersionColumnValue(String versionColumnValue) {
        this.versionColumnValue = versionColumnValue;
    }

    public void setEncoding(String defaultEncoding) {
        this.encoding = defaultEncoding;
    }

    public void setWriteBufferSize(Long writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
    }

    public void setNullMode(String nullMode) {
        this.nullMode = nullMode;
    }

    public void setWalFlag(Boolean walFlag) {
        this.walFlag = walFlag;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public Map<String, Object> getHbaseConfig() {
        return hbaseConfig;
    }
}
