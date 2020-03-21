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
package com.dtstack.flinkx.sqlservercdc.listener;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.sqlservercdc.ChangeTable;
import com.dtstack.flinkx.sqlservercdc.ChangeTablePointer;
import com.dtstack.flinkx.sqlservercdc.Lsn;
import com.dtstack.flinkx.sqlservercdc.SqlServerCdcUtil;
import com.dtstack.flinkx.sqlservercdc.SqlserverCdcEnum;
import com.dtstack.flinkx.sqlservercdc.TableId;
import com.dtstack.flinkx.sqlservercdc.TxLogPosition;
import com.dtstack.flinkx.sqlservercdc.format.SqlserverCdcInputFormat;
import com.dtstack.flinkx.util.Clock;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.Metronome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Date: 2019/12/04
 * Company: www.dtstack.com
 *
 * some code in run() are copied from (https://github.com/debezium/debezium).
 *
 * @author tudou
 */
public class SqlServerCdcListener implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerCdcListener.class);

    private SqlserverCdcInputFormat format;
    private TxLogPosition logPosition;
    private ChangeTable[] tablesSlot;
    private Connection conn;
    private List<String> tableList;
    private Set<Integer> cat;
    private boolean pavingData;
    private Duration pollInterval;

    public SqlServerCdcListener(SqlserverCdcInputFormat format) throws SQLException {
        this.format = format;
        this.conn = format.getConn();
        this.logPosition = format.getLogPosition();
        this.tableList = format.getTableList();
        this.cat = new HashSet<>();
        for (String type : format.getCat().split(ConstantValue.COMMA_SYMBOL)) {
            cat.addAll(SqlserverCdcEnum.transform(type));
        }
        this.pavingData = format.isPavingData();
        this.tablesSlot = SqlServerCdcUtil.getCdcTablesToQuery(conn, format.getDatabaseName(), tableList);
        this.pollInterval = Duration.of(format.getPollInterval(), ChronoUnit.MILLIS);
    }

    @Override
    public void run() {
        LOG.info("SqlServerCdcListener start running.....");
        try {
            Metronome metronome = Metronome.sleeper(pollInterval, Clock.system());
            while (true){

                Lsn currentMaxLsn = SqlServerCdcUtil.getMaxLsn(conn);

                // Shouldn't happen if the agent is running, but it is better to guard against such situation
                if (!currentMaxLsn.isAvailable()) {
                    LOG.warn("No maximum LSN recorded in the database; please ensure that the SQL Server Agent is running");
                    metronome.pause();
                    continue;
                }

                // There is no change in the database
                if (currentMaxLsn.equals(logPosition.getCommitLsn())) {
                    metronome.pause();
                    continue;
                }

                // Reading interval is inclusive so we need to move LSN forward but not for first
                // run as TX might not be streamed completely
                Lsn fromLsn;
                if(logPosition.getCommitLsn().isAvailable()){
                    fromLsn = SqlServerCdcUtil.incrementLsn(conn, logPosition.getCommitLsn());
                }else {
                    fromLsn = logPosition.getCommitLsn();
                }

                ResultSet[] resultSets = SqlServerCdcUtil.getChangesForTables(conn, tablesSlot, fromLsn, currentMaxLsn);
                int tableCount = resultSets.length;
                final ChangeTablePointer[] changeTables = new ChangeTablePointer[tableCount];
                for (int i = 0; i < tableCount; i++) {
                    changeTables[i] = new ChangeTablePointer(tablesSlot[i], resultSets[i]);
                    changeTables[i].next();
                }
                for (;;) {
                    ChangeTablePointer tableWithSmallestLsn = null;
                    for (ChangeTablePointer changeTable: changeTables) {
                        if (changeTable.isCompleted()) {
                            continue;
                        }
                        if (tableWithSmallestLsn == null || changeTable.compareTo(tableWithSmallestLsn) < 0) {
                            tableWithSmallestLsn = changeTable;
                        }
                    }
                    if (tableWithSmallestLsn == null) {
                        break;
                    }

                    if (!(tableWithSmallestLsn.getChangePosition().isAvailable() && tableWithSmallestLsn.getChangePosition().getInTxLsn().isAvailable())) {
                        LOG.error("Skipping change {} as its LSN is NULL which is not expected", tableWithSmallestLsn);
                        tableWithSmallestLsn.next();
                        continue;
                    }
                    // After restart for changes that were executed before the last committed offset
                    if (tableWithSmallestLsn.getChangePosition().compareTo(logPosition) < 0) {
                        LOG.info("Skipping change {} as its position is smaller than the last recorded position {}", tableWithSmallestLsn, logPosition);
                        tableWithSmallestLsn.next();
                        continue;
                    }
                    ChangeTable changeTable = tableWithSmallestLsn.getChangeTable();
                    if (changeTable.getStopLsn().isAvailable() &&
                            changeTable.getStopLsn().compareTo(tableWithSmallestLsn.getChangePosition().getCommitLsn()) <= 0) {
                        LOG.debug("Skipping table change {} as its stop LSN is smaller than the last recorded LSN {}", tableWithSmallestLsn, tableWithSmallestLsn.getChangePosition());
                        tableWithSmallestLsn.next();
                        continue;
                    }
                    int operation = tableWithSmallestLsn.getOperation();
                    if(!cat.contains(operation)){
                        tableWithSmallestLsn.next();
                        continue;
                    }
                    TableId tableId = changeTable.getSourceTableId();
                    Object[] data = tableWithSmallestLsn.getData();

                    if (operation == SqlserverCdcEnum.UPDATE_BEFORE.code) {
                        if (!tableWithSmallestLsn.next() || tableWithSmallestLsn.getOperation() != SqlserverCdcEnum.UPDATE_AFTER.code) {
                            throw new IllegalStateException("The update before event at " + tableWithSmallestLsn.getChangePosition() + " for table " + tableId + " was not followed by after event");
                        }
                    }
                    Object[] dataNext;
                    if(operation == SqlserverCdcEnum.UPDATE_BEFORE.code){
                        dataNext = tableWithSmallestLsn.getData();
                    }else if(operation == SqlserverCdcEnum.DELETE.code){
                        dataNext = data;
                        data = new Object[dataNext.length];
                    }else{
                        dataNext = new Object[data.length];
                    }
                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put("type", SqlserverCdcEnum.getEnum(operation).name.split("_")[0]);
                    map.put("schema", tableId.getSchemaName());
                    map.put("table", tableId.getTableName());
                    map.put("lsn", tableWithSmallestLsn.getChangePosition().getCommitLsn().toString());
                    map.put("ingestion", System.nanoTime());
                    if(pavingData){
                        int i = 0;
                        for (String column : changeTable.getColumnList()) {
                            map.put("before_" + column, SqlServerCdcUtil.clobToString(dataNext[i]));
                            map.put("after_" + column, SqlServerCdcUtil.clobToString(data[i]));
                            i++;
                        }
                    }else{
                        Map<String, Object> before = new LinkedHashMap<>();
                        Map<String, Object> after = new LinkedHashMap<>();
                        int i = 0;
                        for (String column : changeTable.getColumnList()) {
                            before.put(column, SqlServerCdcUtil.clobToString(data[i]));
                            after.put(column, SqlServerCdcUtil.clobToString(dataNext[i]));
                            i++;
                        }
                        map.put("before", before);
                        map.put("after", after);
                    }
                    format.processEvent(map);
                    format.setLogPosition(tableWithSmallestLsn.getChangePosition());
                    tableWithSmallestLsn.next();
                }

                LOG.debug("currentMaxLsn = {}", logPosition);
                logPosition = TxLogPosition.valueOf(currentMaxLsn);
                conn.rollback();
            }
        }catch (Exception e){
            String errorMessage = ExceptionUtil.getErrorMessage(e);
            LOG.error(errorMessage);
            format.processEvent(Collections.singletonMap("e", errorMessage));
        }
    }


}
