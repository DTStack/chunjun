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
package com.dtstack.flinkx.connector.inceptor.sink;

import com.dtstack.flinkx.connector.inceptor.conf.InceptorConf;
import com.dtstack.flinkx.connector.inceptor.dialect.InceptorDialect;
import com.dtstack.flinkx.connector.inceptor.util.InceptorDbUtil;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.throwable.WriteRecordException;
import com.dtstack.flinkx.util.GsonUtil;

import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * OutputFormat for writing data to relational database.
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class InceptorOutputFormat extends JdbcOutputFormat {

    protected static final Logger LOG = LoggerFactory.getLogger(InceptorOutputFormat.class);

    protected static final long serialVersionUID = 1L;

    protected InceptorConf inceptorConf;

    private SimpleDateFormat partitionFormat;

    // 当前分区
    private String currentPartition;

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        int index = 0;
        try {
            switchNextPartiiton(new Date());
            stmtProxy.writeSingleRecordInternal(rowData);
        } catch (Exception e) {
            processWriteException(e, index, rowData);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        try {
            switchNextPartiiton(new Date());
            for (RowData row : rows) {
                stmtProxy.convertToExternal(row);
                stmtProxy.addBatch();
                lastRow = row;
            }
            stmtProxy.executeBatch();
        } catch (Exception e) {
            LOG.warn(
                    "write Multiple Records error, start to rollback connection, first row = {}",
                    rows.size() > 0 ? GsonUtil.GSON.toJson(rows.get(0)) : "null",
                    e);
            throw e;
        } finally {
            // 执行完后清空batch
            stmtProxy.clearBatch();
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        try {
            partitionFormat = getPartitionFormat();
            dbConn = getConnection();
            // 默认关闭事务自动提交，手动控制事务
            // dbConn.setAutoCommit(false);

            initColumnList();
            switchNextPartiiton(new Date());
            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException throwables) {
            throw new IllegalArgumentException("open() failed.", throwables);
        } finally {
            JdbcUtil.commit(dbConn);
        }
    }

    @Override
    public Connection getConnection() {

        return InceptorDbUtil.getConnection(
                inceptorConf, getRuntimeContext().getDistributedCache(), jobId);
    }

    @Override
    protected String prepareTemplates() {
        String singleSql =
                ((InceptorDialect) jdbcDialect)
                        .getInsertPartitionIntoStatement(
                                inceptorConf.getSchema(),
                                inceptorConf.getTable(),
                                inceptorConf.getPartition(),
                                currentPartition,
                                columnNameList.toArray(new String[0]));

        LOG.info("write sql:{}", singleSql);
        return singleSql;
    }

    private SimpleDateFormat getPartitionFormat() {
        if (StringUtils.isBlank(inceptorConf.getPartitionType())) {
            throw new IllegalArgumentException("partitionEnumStr is empty!");
        }
        SimpleDateFormat format;
        switch (inceptorConf.getPartitionType().toUpperCase(Locale.ENGLISH)) {
            case "DAY":
                format = new SimpleDateFormat("yyyyMMdd");
                break;
            case "HOUR":
                format = new SimpleDateFormat("yyyyMMddHH");
                break;
            case "MINUTE":
                format = new SimpleDateFormat("yyyyMMddHHmm");
                break;
            default:
                throw new UnsupportedOperationException(
                        "partitionEnum = " + inceptorConf.getPartitionType() + " is undefined!");
        }
        TimeZone timeZone = TimeZone.getDefault();
        LOG.info("timeZone = {}", timeZone);
        format.setTimeZone(timeZone);
        return format;
    }

    private void switchNextPartiiton(Date currentData) throws SQLException {
        String newPartition = partitionFormat.format(currentData);
        if (StringUtils.isBlank(currentPartition) || !currentPartition.equals(newPartition)) {
            LOG.info(
                    "switch old partition {}  to new partition {}", currentPartition, newPartition);
            if (stmtProxy != null) {
                stmtProxy.close();
            }
            currentPartition = newPartition;
            buildStmtProxy();
        }
    }

    public void setInceptorConf(InceptorConf inceptorConf) {
        this.inceptorConf = inceptorConf;
    }
}
