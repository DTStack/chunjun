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
package com.dtstack.chunjun.connector.pgwal.util;

import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.TelnetUtil;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.core.ServerVersion;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.ReplicationSlotInfo;
import org.postgresql.replication.fluent.logical.ChainedLogicalCreateSlotBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** */
public class PGUtil {
    public static final String SLOT_PRE = "flinkx_";
    public static final String DRIVER_NAME = "org.postgresql.Driver";
    public static final int RETRY_TIMES = 3;
    public static final int SLEEP_TIME = 2000;
    public static final String PUBLICATION_NAME = "dtstack_flinkx";
    public static final String QUERY_LEVEL = "SHOW wal_level;";
    public static final String QUERY_MAX_SLOT = "SHOW max_replication_slots;";
    public static final String QUERY_SLOT = "SELECT * FROM pg_replication_slots;";
    public static final String QUERY_TABLE_REPLICA_IDENTITY =
            "SELECT relreplident FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n "
                    + "ON c.relnamespace=n.oid WHERE n.nspname='%s' and c.relname='%s';";
    public static final String UPDATE_REPLICA_IDENTITY = "ALTER TABLE %s REPLICA IDENTITY FULL;";
    public static final String QUERY_PUBLICATION =
            "SELECT COUNT(1) FROM pg_publication WHERE pubname = '%s';";
    public static final String CREATE_PUBLICATION = "CREATE PUBLICATION %s FOR ALL TABLES;";
    public static final String QUERY_TYPES =
            "SELECT t.oid AS oid, t.typname AS name FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n "
                    + "ON (t.typnamespace = n.oid) WHERE n.nspname != 'pg_toast' AND t.typcategory <> 'A';";
    private static final Logger LOG = LoggerFactory.getLogger(PGUtil.class);
    private static final Object lock = new Object();

    public static String formatTableName(String schemaName, String tableName) {
        StringBuilder stringBuilder = new StringBuilder();
        if (tableName.contains(ConstantValue.POINT_SYMBOL)) {
            return tableName;
        } else {
            return stringBuilder
                    .append(schemaName)
                    .append(ConstantValue.POINT_SYMBOL)
                    .append(tableName)
                    .toString();
        }
    }

    public static PGConnection getConnection(String jdbcUrl, String username, String password)
            throws SQLException {
        Connection dbConn;
        ClassUtil.forName(DRIVER_NAME, PGUtil.class.getClassLoader());
        Properties props = new Properties();
        PGProperty.USER.set(props, username);
        PGProperty.PASSWORD.set(props, password);
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10");
        PGProperty.SOCKET_TIMEOUT.set(props, "60");
        PGProperty.CONNECT_TIMEOUT.set(props, "60");
        PGProperty.LOGIN_TIMEOUT.set(props, "60");
        PGProperty.LOGGER_LEVEL.set(props, "DEBUG");
        synchronized (lock) {
            TelnetUtil.telnet(jdbcUrl);
            dbConn = DriverManager.getConnection(jdbcUrl, props);
        }

        return dbConn.unwrap(PgConnection.class);
    }

    public static ReplicationSlotInfoWrapper checkPostgres(
            PgConnection conn, boolean allowCreateSlot, String slotName, List<String> tableList) {
        ResultSet resultSet = null;
        ReplicationSlotInfoWrapper slotInfoWrapper = null;

        try {
            // 1. check postgres version
            // this Judge maybe not need?
            if (!conn.haveMinimumServerVersion(ServerVersion.v10)) {
                String version = conn.getDBVersionNumber();
                LOG.error("postgres version must > 10, current = [{}]", version);
                throw new UnsupportedOperationException(
                        "postgres version must >= 10, current = " + version);
            }

            // 2. check postgres wal_level
            resultSet = conn.execSQLQuery(QUERY_LEVEL);
            resultSet.next();
            String wal_level = resultSet.getString(1);
            if (!"logical".equalsIgnoreCase(wal_level)) {
                LOG.error("postgres wal_level must be logical, current = [{}]", wal_level);
                throw new UnsupportedOperationException(
                        "postgres wal_level must be logical, current = " + wal_level);
            }

            // 3.check postgres slot
            resultSet = conn.execSQLQuery(QUERY_MAX_SLOT);
            resultSet.next();
            int maxSlot = resultSet.getInt(1);
            int slotCount = 0;
            resultSet = conn.execSQLQuery(QUERY_SLOT);

            while (resultSet.next()) {
                ReplicationSlotInfoWrapper wrapper =
                        new ReplicationSlotInfoWrapper(
                                resultSet.getString("slot_name"),
                                resultSet.getString("active"),
                                resultSet.getString("confirmed_flush_lsn"));

                if (wrapper.getSlotName().equalsIgnoreCase(slotName) && !wrapper.isActive()) {
                    //                slotInfoWrapper.setSlotType(resultSet.getString("slot_type"));
                    //                slotInfoWrapper.setDatabase(resultSet.getString("database"));
                    //
                    // slotInfoWrapper.setTemporary(resultSet.getString("temporary"));
                    //
                    // slotInfoWrapper.setRestartLsn(resultSet.getString("restart_lsn"));
                    slotInfoWrapper = wrapper;
                    break;
                }
                slotCount++;
            }

            if (slotInfoWrapper == null) {
                if (!allowCreateSlot) {
                    String msg =
                            String.format(
                                    "there is no available slot named [%s], please check whether slotName[%s] is correct, or set allowCreateSlot = true",
                                    slotName, slotName);
                    LOG.error(msg);
                    throw new UnsupportedOperationException(msg);
                } else if (slotCount >= maxSlot) {
                    LOG.error(
                            "the number of slot reaches max_replication_slots[{}], please turn up max_replication_slots or remove unused slot",
                            maxSlot);
                    throw new UnsupportedOperationException(
                            "the number of slot reaches max_replication_slots["
                                    + maxSlot
                                    + "], please turn up max_replication_slots or remove unused slot");
                }
            }

            // 4.check table replica identity
            for (String table : tableList) {
                // schema.tableName
                String[] tables = table.split("\\.");
                resultSet =
                        conn.execSQLQuery(
                                String.format(QUERY_TABLE_REPLICA_IDENTITY, tables[0], tables[1]));
                resultSet.next();
                if (!"f".equals(resultSet.getString(1))) {
                    LOG.warn("update {} replica identity, set to full", table);
                    conn.createStatement().execute(String.format(UPDATE_REPLICA_IDENTITY, table));
                }
            }

            // 5.check publication
            resultSet = conn.execSQLQuery(String.format(QUERY_PUBLICATION, PUBLICATION_NAME));
            resultSet.next();
            long count = resultSet.getLong(1);
            if (count == 0L) {
                LOG.warn(
                        "no publication named [{}] existed, flinkx will create one",
                        PUBLICATION_NAME);
                conn.createStatement().execute(String.format(CREATE_PUBLICATION, PUBLICATION_NAME));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (null != resultSet) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    LOG.warn("Close statement error:{}", ExceptionUtil.getErrorMessage(e));
                }
            }
        }
        return slotInfoWrapper;
    }

    public static ReplicationSlotInfoWrapper createSlot(
            PGConnection conn, String slotName, Boolean isTemp) throws SQLException {
        ChainedLogicalCreateSlotBuilder builder =
                conn.getReplicationAPI()
                        .createReplicationSlot()
                        .logical()
                        .withSlotName(slotName)
                        .withOutputPlugin("pgoutput");
        if (isTemp) {
            builder.withTemporaryOption();
        }
        ReplicationSlotInfo replicationSlotInfo = builder.make();

        ReplicationSlotInfoWrapper wrapper =
                new ReplicationSlotInfoWrapper(replicationSlotInfo, isTemp);
        return wrapper;
    }

    public static Map<Integer, String> queryTypes(PgConnection conn) throws SQLException {
        Map<Integer, String> map = new HashMap<>(512);
        ResultSet resultSet = null;
        try {
            resultSet = conn.execSQLQuery(QUERY_TYPES);
            while (resultSet.next()) {
                long oid = resultSet.getLong("oid");
                String typeName = resultSet.getString("name");
                map.put((int) oid, typeName);
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
                ;
            }
        }
        return map;
    }
}
