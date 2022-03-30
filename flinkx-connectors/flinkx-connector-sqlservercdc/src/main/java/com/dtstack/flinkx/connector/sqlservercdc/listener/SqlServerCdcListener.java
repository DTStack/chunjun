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
package com.dtstack.flinkx.connector.sqlservercdc.listener;

import com.dtstack.flinkx.connector.sqlservercdc.entity.ChangeTable;
import com.dtstack.flinkx.connector.sqlservercdc.entity.ChangeTablePointer;
import com.dtstack.flinkx.connector.sqlservercdc.entity.Lsn;
import com.dtstack.flinkx.connector.sqlservercdc.entity.SqlServerCdcEnum;
import com.dtstack.flinkx.connector.sqlservercdc.entity.SqlServerCdcEventRow;
import com.dtstack.flinkx.connector.sqlservercdc.entity.TableId;
import com.dtstack.flinkx.connector.sqlservercdc.entity.TxLogPosition;
import com.dtstack.flinkx.connector.sqlservercdc.inputFormat.SqlServerCdcInputFormat;
import com.dtstack.flinkx.connector.sqlservercdc.util.SqlServerCdcUtil;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.throwable.WriteRecordException;
import com.dtstack.flinkx.util.Clock;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.Metronome;
import com.dtstack.flinkx.util.SnowflakeIdWorker;

import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Date: 2019/12/04 Company: www.dtstack.com
 *
 * <p>some code in run() are copied from (https://github.com/debezium/debezium).
 *
 * @author tudou
 */
public class SqlServerCdcListener implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerCdcListener.class);

    private SqlServerCdcInputFormat format;
    private TxLogPosition logPosition;
    private ChangeTable[] tablesSlot;
    private Connection conn;
    private List<String> tableList;
    private Set<Integer> cat;
    private Duration pollInterval;
    private SnowflakeIdWorker idWorker;
    private AbstractCDCRowConverter rowConverter;

    public SqlServerCdcListener(SqlServerCdcInputFormat format) throws SQLException {
        this.format = format;
        this.conn = format.getConn();
        this.logPosition = format.getLogPosition();
        this.tableList = format.sqlserverCdcConf.getTableList();
        this.cat = new HashSet<>();
        for (String type : format.sqlserverCdcConf.getCat().split(ConstantValue.COMMA_SYMBOL)) {
            cat.addAll(SqlServerCdcEnum.transform(type));
        }
        this.tablesSlot =
                SqlServerCdcUtil.getCdcTablesToQuery(
                        conn, format.sqlserverCdcConf.getDatabaseName(), tableList);
        this.pollInterval =
                Duration.of(format.sqlserverCdcConf.getPollInterval(), ChronoUnit.MILLIS);
        idWorker = new SnowflakeIdWorker(1, 1);
        this.rowConverter = format.getRowConverter();
    }

    @Override
    public void run() {
        LOG.info("SqlServerCdcListener start running.....");
        Metronome metronome = Metronome.sleeper(pollInterval, Clock.system());
        while (true) {
            try {
                Lsn currentMaxLsn = SqlServerCdcUtil.getMaxLsn(conn);

                // Shouldn't happen if the agent is running, but it is better to guard against such
                // situation
                if (!currentMaxLsn.isAvailable()) {
                    LOG.warn(
                            "No maximum LSN recorded in the database; please ensure that the SQL Server Agent is running");
                    metronome.pause();
                    if (format.sqlserverCdcConf.isAutoResetConnection()) {
                        resetConnection();
                    }
                    continue;
                }

                // There is no change in the database
                if (currentMaxLsn.equals(logPosition.getCommitLsn())) {
                    metronome.pause();
                    if (format.sqlserverCdcConf.isAutoResetConnection()) {
                        resetConnection();
                    }
                    continue;
                }

                final ChangeTablePointer[] changeTables = getChangeTables(currentMaxLsn);
                readData(changeTables);

                LOG.debug("currentMaxLsn = {}", logPosition);
                logPosition = TxLogPosition.valueOf(currentMaxLsn);
                if (!format.sqlserverCdcConf.isAutoCommit()) {
                    conn.rollback();
                }
            } catch (Exception e) {
                String errorMessage = ExceptionUtil.getErrorMessage(e);
                LOG.error(errorMessage, e);
            }
        }
    }

    private void readData(ChangeTablePointer[] changeTables) throws Exception {
        for (; ; ) {
            ChangeTablePointer tableWithSmallestLsn = getTableWithSmallestLsn(changeTables);
            if (tableWithSmallestLsn == null) {
                break;
            }

            if (!(tableWithSmallestLsn.getChangePosition().isAvailable()
                    && tableWithSmallestLsn.getChangePosition().getInTxLsn().isAvailable())) {
                LOG.error(
                        "Skipping change {} as its LSN is NULL which is not expected",
                        tableWithSmallestLsn);
                tableWithSmallestLsn.next();
                continue;
            }

            // After restart for changes that were executed before the last committed offset
            if (tableWithSmallestLsn.getChangePosition().compareTo(logPosition) < 0) {
                LOG.info(
                        "Skipping change {} as its position is smaller than the last recorded position {}",
                        tableWithSmallestLsn,
                        logPosition);
                tableWithSmallestLsn.next();
                continue;
            }

            ChangeTable changeTable = tableWithSmallestLsn.getChangeTable();
            if (changeTable.getStopLsn().isAvailable()
                    && changeTable
                                    .getStopLsn()
                                    .compareTo(
                                            tableWithSmallestLsn.getChangePosition().getCommitLsn())
                            <= 0) {
                LOG.debug(
                        "Skipping table change {} as its stop LSN is smaller than the last recorded LSN {}",
                        tableWithSmallestLsn,
                        tableWithSmallestLsn.getChangePosition());
                tableWithSmallestLsn.next();
                continue;
            }

            int operation = tableWithSmallestLsn.getOperation();
            if (!cat.contains(operation)) {
                tableWithSmallestLsn.next();
                continue;
            }

            Object[] dataPrev = null;
            TableId tableId = changeTable.getSourceTableId();
            if (operation == SqlServerCdcEnum.UPDATE_BEFORE.code) {
                dataPrev = tableWithSmallestLsn.getData();
                if (!tableWithSmallestLsn.next()
                        || tableWithSmallestLsn.getOperation()
                                != SqlServerCdcEnum.UPDATE_AFTER.code) {
                    throw new IllegalStateException(
                            "The update before event at "
                                    + tableWithSmallestLsn.getChangePosition()
                                    + " for table "
                                    + tableId
                                    + " was not followed by after event");
                }
            }

            Object[] data = tableWithSmallestLsn.getData();
            List<String> columnTypes = tableWithSmallestLsn.getTypes();

            if (operation == SqlServerCdcEnum.DELETE.code) {
                dataPrev = data;
                data = new Object[dataPrev.length];
            } else if (operation != SqlServerCdcEnum.UPDATE_BEFORE.code) {
                dataPrev = new Object[data.length];
            }

            buildResult(
                    changeTable,
                    tableId,
                    data,
                    dataPrev,
                    operation,
                    tableWithSmallestLsn,
                    columnTypes);
            format.setLogPosition(tableWithSmallestLsn.getChangePosition());
            tableWithSmallestLsn.next();
        }
    }

    private void buildResult(
            ChangeTable changeTable,
            TableId tableId,
            Object[] data,
            Object[] dataPrev,
            int operation,
            ChangeTablePointer tableWithSmallestLsn,
            List<String> types)
            throws Exception {
        String type = SqlServerCdcEnum.getEnum(operation).name.split("_")[0];
        String schema = tableId.getSchemaName();
        String table = tableId.getTableName();
        String lsn = tableWithSmallestLsn.getChangePosition().getCommitLsn().toString();
        SqlServerCdcEventRow sqlServerCdcEventRow =
                new SqlServerCdcEventRow(
                        type,
                        schema,
                        table,
                        lsn,
                        idWorker.nextId(),
                        changeTable,
                        data,
                        dataPrev,
                        types);
        try {
            LinkedList<RowData> rowDatalist = rowConverter.toInternal(sqlServerCdcEventRow);
            RowData rowData;
            while ((rowData = rowDatalist.poll()) != null) {
                format.getQueue().put(rowData);
            }
        } catch (Exception e) {
            throw new WriteRecordException("", e, 0, sqlServerCdcEventRow);
        }
    }

    private ChangeTablePointer[] getChangeTables(Lsn currentMaxLsn) throws SQLException {
        // Reading interval is inclusive so we need to move LSN forward but not for first
        // run as TX might not be streamed completely
        Lsn fromLsn = getFromLsn();

        SqlServerCdcUtil.StatementResult[] resultSets =
                SqlServerCdcUtil.getChangesForTables(conn, tablesSlot, fromLsn, currentMaxLsn);
        int tableCount = resultSets.length;
        ChangeTablePointer[] changeTables = new ChangeTablePointer[tableCount];
        for (int i = 0; i < tableCount; i++) {
            changeTables[i] = new ChangeTablePointer(tablesSlot[i], resultSets[i]);
            changeTables[i].next();
        }

        return changeTables;
    }

    private ChangeTablePointer getTableWithSmallestLsn(ChangeTablePointer[] changeTables)
            throws SQLException {
        ChangeTablePointer tableWithSmallestLsn = null;
        for (ChangeTablePointer changeTable : changeTables) {
            if (changeTable.isCompleted()) {
                continue;
            }
            if (tableWithSmallestLsn == null || changeTable.compareTo(tableWithSmallestLsn) < 0) {
                tableWithSmallestLsn = changeTable;
            }
        }

        return tableWithSmallestLsn;
    }

    private Lsn getFromLsn() throws SQLException {
        if (logPosition.getCommitLsn().isAvailable()) {
            return SqlServerCdcUtil.incrementLsn(conn, logPosition.getCommitLsn());
        } else {
            return logPosition.getCommitLsn();
        }
    }

    private void resetConnection() throws SQLException {
        if (conn != null) {
            conn.close();
        }
        Connection connection =
                SqlServerCdcUtil.getConnection(
                        format.sqlserverCdcConf.getUrl(),
                        format.sqlserverCdcConf.getUsername(),
                        format.sqlserverCdcConf.getPassword());
        connection.setAutoCommit(format.sqlserverCdcConf.isAutoCommit());
        conn = connection;
    }
}
