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
package com.dtstack.flinkx.pgwal;

import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import org.postgresql.PGProperty;
import org.postgresql.core.ServerVersion;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.ReplicationSlotInfo;
import org.postgresql.replication.fluent.logical.ChainedLogicalCreateSlotBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 2019/12/13
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PgWalUtil {

    public static final String DRIVER = "org.postgresql.Driver";
    public static final String SLOT_PRE = "flinkx_";
    public static final String PUBLICATION_NAME = "dtstack_flinkx";
    public static final String QUERY_LEVEL = "SHOW wal_level;";
    public static final String QUERY_MAX_SLOT = "SHOW max_replication_slots;";
    public static final String QUERY_SLOT = "SELECT * FROM pg_replication_slots;";
    public static final String QUERY_TABLE_REPLICA_IDENTITY = "SELECT relreplident FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid WHERE n.nspname='%s' and c.relname='%s';";
    public static final String UPDATE_REPLICA_IDENTITY = "ALTER TABLE %s REPLICA IDENTITY FULL;";
    public static final String QUERY_PUBLICATION = "SELECT COUNT(1) FROM pg_publication WHERE pubname = '%s';";
    public static final String CREATE_PUBLICATION = "CREATE PUBLICATION %s FOR ALL TABLES;";
    public static final String QUERY_TYPES = "SELECT t.oid AS oid, t.typname AS name FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) WHERE n.nspname != 'pg_toast' AND t.typcategory <> 'A';";
    private static final Logger LOG = LoggerFactory.getLogger(PgWalUtil.class);

    public static PgRelicationSlot checkPostgres(PgConnection conn, boolean allowCreateSlot, String slotName, List<String> tableList) throws Exception{
        ResultSet resultSet;
        PgRelicationSlot availableSlot = null;

        //1. check postgres version
        // this Judge maybe not need?
        if (!conn.haveMinimumServerVersion(ServerVersion.v10)){
            String version = conn.getDBVersionNumber();
            LOG.error("postgres version must > 10, current = [{}]", version);
            throw new UnsupportedOperationException("postgres version must >= 10, current = " + version);
        }

        //2. check postgres wal_level
        resultSet = conn.execSQLQuery(QUERY_LEVEL);
        resultSet.next();
        String wal_level = resultSet.getString(1);
        if(!"logical".equalsIgnoreCase(wal_level)){
            LOG.error("postgres wal_level must be logical, current = [{}]", wal_level);
            throw new UnsupportedOperationException("postgres wal_level must be logical, current = " + wal_level);
        }

        //3.check postgres slot
        resultSet = conn.execSQLQuery(QUERY_MAX_SLOT);
        resultSet.next();
        int maxSlot = resultSet.getInt(1);
        int slotCount = 0;
        resultSet = conn.execSQLQuery(QUERY_SLOT);
        while(resultSet.next()){
            PgRelicationSlot slot = new PgRelicationSlot();
            String name = resultSet.getString("slot_name");
            slot.setSlotName(name);
            slot.setActive(resultSet.getString("active"));

            if(name.equalsIgnoreCase(slotName) && slot.isNotActive()){
                slot.setPlugin(resultSet.getString("plugin"));
                slot.setSlotType(resultSet.getString("slot_type"));
                slot.setDatoid(resultSet.getInt("datoid"));
                slot.setDatabase(resultSet.getString("database"));
                slot.setTemporary(resultSet.getString("temporary"));
                slot.setActivePid(resultSet.getInt("active_pid"));
                slot.setXmin(resultSet.getString("xmin"));
                slot.setCatalogXmin(resultSet.getString("catalog_xmin"));
                slot.setRestartLsn(resultSet.getString("restart_lsn"));
                slot.setConfirmedFlushLsn(resultSet.getString("confirmed_flush_lsn"));
                availableSlot = slot;
                break;
            }
            slotCount++;
        }

        if(availableSlot == null){
            if(!allowCreateSlot){
                String msg = String.format("there is no available slot named [%s], please check whether slotName[%s] is correct, or set allowCreateSlot = true", slotName, slotName);
                LOG.error(msg);
                throw new UnsupportedOperationException(msg);
            }else if(slotCount >= maxSlot){
                LOG.error("the number of slot reaches max_replication_slots[{}], please turn up max_replication_slots or remove unused slot", maxSlot);
                throw new UnsupportedOperationException("the number of slot reaches max_replication_slots[" + maxSlot + "], please turn up max_replication_slots or remove unused slot");
            }
        }

        //4.check table replica identity
        for (String table : tableList) {
            //schema.tableName
            String[] tables = table.split("\\.");
            resultSet = conn.execSQLQuery(String.format(QUERY_TABLE_REPLICA_IDENTITY, tables[0], tables[1]));
            resultSet.next();
            String identity = parseReplicaIdentity(resultSet.getString(1));
            if(!"full".equals(identity)){
                LOG.warn("update {} replica identity, set {} to full", table, identity);
                conn.createStatement().execute(String.format(UPDATE_REPLICA_IDENTITY, table));
            }
        }

        //5.check publication
        resultSet = conn.execSQLQuery(String.format(QUERY_PUBLICATION, PUBLICATION_NAME));
        resultSet.next();
        long count = resultSet.getLong(1);
        if(count == 0L){
            LOG.warn("no publication named [{}] existed, flinkx will create one", PUBLICATION_NAME);
            conn.createStatement().execute(String.format(CREATE_PUBLICATION, PUBLICATION_NAME));
        }

        closeDBResources(resultSet, null, null, false);
        return availableSlot;
    }

    public static PgRelicationSlot createSlot(PgConnection conn, String slotName, boolean temporary) throws SQLException{
        ChainedLogicalCreateSlotBuilder builder = conn.getReplicationAPI()
                                                        .createReplicationSlot()
                                                        .logical()
                                                        .withSlotName(slotName)
                                                        .withOutputPlugin("pgoutput");
        if(temporary){
            builder.withTemporaryOption();
        }
        ReplicationSlotInfo replicationSlotInfo = builder.make();
        PgRelicationSlot slot = new PgRelicationSlot();
        slot.setSlotName(slotName);
        slot.setConfirmedFlushLsn(replicationSlotInfo.getConsistentPoint().asString());
        slot.setPlugin(replicationSlotInfo.getOutputPlugin());
        return slot;
    }

    public static Map<Integer, String> queryTypes(PgConnection conn) throws SQLException{
        Map<Integer, String> map = new HashMap<>(512);
        ResultSet resultSet = conn.execSQLQuery(QUERY_TYPES);
        while (resultSet.next()){
            int oid = (int) resultSet.getLong("oid");
            String typeName = resultSet.getString("name");
            map.put(oid, typeName);
        }
        closeDBResources(resultSet, null, null, false);
        return map;
    }

    public static String parseReplicaIdentity(String s) {
        switch (s) {
            case "n":
                return "nothing";
            case "d":
                return "default";
            case "i" :
                return "index";
            case "f" :
                return "full";
            default:
                return "unknown";
        }
    }

    /**
     * 获取jdbc连接(超时10S)
     * @param url       url
     * @param username  账号
     * @param password  密码
     * @return
     * @throws SQLException
     */
    public static PgConnection getConnection(String url, String username, String password) throws SQLException {
        Connection dbConn;
        ClassUtil.forName(PgWalUtil.DRIVER, PgWalUtil.class.getClassLoader());
        Properties props = new Properties();
        PGProperty.USER.set(props, username);
        PGProperty.PASSWORD.set(props, password);
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        //postgres version must > 10
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10");
        synchronized (ClassUtil.LOCK_STR) {
            DriverManager.setLoginTimeout(10);
            // telnet
            TelnetUtil.telnet(url);
            dbConn = DriverManager.getConnection(url, props);
        }

        return dbConn.unwrap(PgConnection.class);
    }

    /**
     * 关闭连接资源
     *
     * @param rs     ResultSet
     * @param stmt   Statement
     * @param conn   Connection
     * @param commit
     */
    public static void closeDBResources(ResultSet rs, Statement stmt, Connection conn, boolean commit) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOG.warn("Close resultSet error: {}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                LOG.warn("Close statement error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (null != conn) {
            try {
                if (commit && !conn.isClosed()) {
                    conn.commit();
                }
                conn.close();
            } catch (SQLException e) {
                LOG.warn("Close connection error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }
    }

}
