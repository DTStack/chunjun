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

package com.dtstack.chunjun.connector.hive3.sink;

import com.dtstack.chunjun.connector.hive3.util.Hive3Util;
import com.dtstack.chunjun.security.KerberosUtil;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.security.AnnotatedSecurityInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.StrictDelimitedInputWriter;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class HdfsTransactionOutputFormat extends HdfsOrcOutputFormat {

    private static final long serialVersionUID = 2789150801491048466L;

    // hive3
    private HiveConf hiveConf;
    private StrictDelimitedInputWriter wr;
    private HiveStreamingConnection connection;

    public static final String COLUMN_FIELD_DELIMITER = ",";
    // 当前事务是否已开启
    private volatile boolean transactionStart;

    @Override
    protected void openSource() {
        try {
            if (hdfsConfig.isTransaction()) {
                SecurityUtil.setSecurityInfoProviders(new AnnotatedSecurityInfo());
                String currentUser;
                try {
                    currentUser = UserGroupInformation.getCurrentUser().getUserName();
                } catch (IOException e) {
                    throw new ChunJunRuntimeException("");
                }
                Object hadoopUser = hdfsConfig.getHadoopConfig().get(HADOOP_USER_NAME);
                // 如果配置的 hadoop 用户不为空，那么设置配置中的用户。
                if (hadoopUser != null && StringUtils.isNotEmpty(hadoopUser.toString())) {
                    currentUser = hadoopUser.toString();
                }
                conf =
                        Hive3Util.getConfiguration(
                                hdfsConfig.getHadoopConfig(), hdfsConfig.getDefaultFS());
                try {
                    // overwrite 需要用到这个 fs 去清空 hdfs 目录.
                    fs =
                            Hive3Util.getFileSystem(
                                    hdfsConfig.getHadoopConfig(),
                                    hdfsConfig.getDefaultFS(),
                                    currentUser,
                                    null,
                                    jobId,
                                    String.valueOf(taskNumber));
                } catch (Exception e) {
                    throw new RuntimeException("Get FileSystem error", e);
                }
                if (ugi == null) {
                    getUgi();
                }
                ugi.doAs(
                        (PrivilegedAction<Void>)
                                () -> {
                                    try {
                                        hiveConf = new HiveConf(conf, Hive.class);
                                        if (openKerberos) {
                                            setMetaStoreKerberosConf();
                                        }
                                        wr =
                                                StrictDelimitedInputWriter.newBuilder()
                                                        .withFieldDelimiter(',')
                                                        .build();
                                    } catch (Exception e) {
                                        throw new RuntimeException("init client failed", e);
                                    }
                                    return null;
                                });
            }
        } catch (Exception e) {
            throw new ChunJunRuntimeException("{}", e);
        }
    }

    public void getUgi() throws IOException {
        openKerberos = Hive3Util.isOpenKerberos(hdfsConfig.getHadoopConfig());
        String currentUser = UserGroupInformation.getCurrentUser().getUserName();
        Object hadoopUser = hdfsConfig.getHadoopConfig().get(HADOOP_USER_NAME);
        if (hadoopUser != null && StringUtils.isNotEmpty(hadoopUser.toString())) {
            currentUser = hadoopUser.toString();
        }
        if (openKerberos) {
            ugi =
                    Hive3Util.getUGI(
                            hdfsConfig.getHadoopConfig(),
                            hdfsConfig.getDefaultFS(),
                            null,
                            jobId,
                            String.valueOf(taskNumber));
            log.info("user:{}, ", ugi.getShortUserName());
        } else {
            ugi = UserGroupInformation.createRemoteUser(currentUser);
        }
    }

    /** hiveMetaStore 也开启 kerberos 验证，此处设置认证 metastore. */
    private void setMetaStoreKerberosConf() {
        String keytabFileName = KerberosUtil.getPrincipalFileName(hdfsConfig.getHadoopConfig());
        keytabFileName =
                KerberosUtil.loadFile(
                        hdfsConfig.getHadoopConfig(),
                        keytabFileName,
                        null,
                        jobId,
                        String.valueOf(taskNumber));
        String principal = MapUtils.getString(hdfsConfig.getHadoopConfig(), KEY_PRINCIPAL);
        String saslEnabled =
                MapUtils.getString(hdfsConfig.getHadoopConfig(), "hive.metastore.sasl.enabled");
        if (null == saslEnabled) {
            saslEnabled = "false";
        }
        if (hdfsConfig.getHadoopConfig().get("hive.metastore.kerberos.principal") == null) {
            hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, principal);
        }
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keytabFileName);
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, saslEnabled);
    }

    @Override
    public void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        String rowString;
        try {
            rowString = getDataFromRow(rowData);
        } catch (Exception e) {
            throw new WriteRecordException(String.format("数据写入hdfs异常，row:{%s}", rowData), e);
        }
        ugi.doAs(
                (PrivilegedAction<Void>)
                        () -> {
                            try {
                                if (connection == null) {
                                    connection = getConnection();
                                }
                                if (!transactionStart) {
                                    connection.beginTransaction();
                                    transactionStart = true;
                                }
                                connection.write(rowString.getBytes());
                                rowsOfCurrentBlock++;
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "writeSingleRecordInternal write data failed, data : "
                                                + rowString,
                                        e);
                            }
                            return null;
                        });
        lastRow = rowData;
        lastWriteTime = System.currentTimeMillis();
    }

    private String getDataFromRow(RowData rowData) throws WriteRecordException {

        List<Object> list = new ArrayList<>();
        try {
            rowConverter.toExternal(rowData, list);
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < fullColumnNameList.size(); i++) {
                Object data = list.get(i);
                if (data != null) {
                    stringBuilder.append(data);
                }
                if (i != fullColumnNameList.size() - 1) {
                    stringBuilder.append(COLUMN_FIELD_DELIMITER);
                }
            }
            return stringBuilder.toString();
        } catch (Exception e) {
            String errorMessage =
                    Hive3Util.parseErrorMsg(
                            String.format("writer hdfs error，rowData:{%s}", rowData),
                            ExceptionUtil.getErrorMessage(e));
            throw new WriteRecordException(errorMessage, e, -1, rowData);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        // hive3 事务表
        if (hdfsConfig.isTransaction()) {
            ugi.doAs(
                    (PrivilegedAction<Void>)
                            () -> {
                                try {
                                    if (connection == null) {
                                        connection = getConnection();
                                    }
                                    if (!transactionStart) {
                                        connection.beginTransaction();
                                        transactionStart = true;
                                    }
                                    String rowString;
                                    for (RowData rowData : rows) {
                                        rowString = getDataFromRow(rowData);
                                        connection.write(rowString.getBytes());
                                    }
                                    if (rows != null && rows.size() > 0) {
                                        // 取最后一条数据
                                        lastRow = rows.get(rows.size() - 1);
                                        rowsOfCurrentBlock += rows.size();
                                        lastWriteTime = System.currentTimeMillis();
                                    }
                                } catch (Throwable e) {
                                    log.error("writeMultipleRecordsInternal 方法, 写入多条数据失败", e);
                                    throw new RuntimeException("WRITER DATA ERROR", e);
                                }
                                return null;
                            });
        }
    }

    private HiveStreamingConnection getConnection() {
        HiveStreamingConnection.Builder builder =
                HiveStreamingConnection.newBuilder()
                        .withDatabase(hdfsConfig.getSchema())
                        .withTable(hdfsConfig.getTable())
                        .withAgentInfo("chunjun3_hive3writer_" + Thread.currentThread().getName())
                        .withTransactionBatchSize(hdfsConfig.getBatchSize()) // 何时触发压缩
                        .withRecordWriter(wr)
                        .withHiveConf(hiveConf);
        if (StringUtils.isNotEmpty(hdfsConfig.getPartitionName())) {
            String[] multiPartitionName = {hdfsConfig.getPartitionName()};
            if (hdfsConfig.getPartitionName().contains("/")) {
                multiPartitionName = hdfsConfig.getPartitionName().split("/");
            }
            List<String> partitions = Arrays.asList(multiPartitionName);
            builder.withStaticPartitionValues(partitions);
        }
        HiveStreamingConnection connect;
        try {
            connect = builder.connect();
        } catch (Throwable e) { // Exception 有可能捕获不到
            log.error("HdfsOrcOutputFormat getConnection 方法, 连接 Hive Metastore 失败", e);
            String metastoreUri = hiveConf.get("hive.metastore.uris");
            throw new RuntimeException(
                    "Error connecting to Hive Metastore URI: "
                            + metastoreUri
                            + ". "
                            + e.getMessage(),
                    e);
        }
        return connect;
    }

    @Override
    protected void checkOutputDir() {
        // hive3 transaction table,doNothing
    }

    @Override
    protected void deleteTmpDataDir() {
        // hive3 transaction table,doNothing
    }

    @Override
    protected void preCommit() {
        snapshotWriteCounter.add(rowsOfCurrentBlock);
        rowsOfCurrentBlock = 0;
        formatState.setJobId(jobId);
    }

    @Override
    public void commit(long checkpointId) {
        if (connection != null && transactionStart) {
            try {
                connection.commitTransaction();
            } catch (StreamingException e) {
                try {
                    connection.abortTransaction();
                } catch (StreamingException ex) {
                    throw new ChunJunRuntimeException("hive3 abort transaction failed");
                }
                throw new ChunJunRuntimeException("hive3 commit transaction failed");
            } finally {
                transactionStart = false;
            }
        }
    }

    @Override
    public void rollback(long checkpointId) {
        try {
            if (connection != null && transactionStart) {
                connection.abortTransaction();
            }
        } catch (StreamingException e) {
            throw new ChunJunRuntimeException("hive3 commit transaction failed");
        } finally {
            transactionStart = false;
        }
    }

    @Override
    public void closeInternal() {
        snapshotWriteCounter.add(rowsOfCurrentBlock);
        rowsOfCurrentBlock = 0;
        closeSource();
    }

    @Override
    protected void closeSource() {
        ugi.doAs(
                (PrivilegedAction<Void>)
                        () -> {
                            if (connection != null) {
                                try {
                                    connection.commitTransaction();
                                } catch (StreamingException e) {
                                    throw new ChunJunRuntimeException(
                                            "hive3 commit transaction failed");
                                }
                            }
                            if (connection != null) {
                                connection.close();
                                connection = null;
                            }
                            return null;
                        });
    }

    @Override
    public void finalizeGlobal(int parallelism) {
        // hive3 transaction table,doNothing
    }

    @Override
    protected void deleteDataDir() {
        deleteDirectory(outputFilePath, false);
    }
}
