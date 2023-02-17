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
package com.dtstack.chunjun.connector.binlog.listener;

import com.alibaba.otter.canal.parse.driver.mysql.packets.server.FieldPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class BinlogJournalValidator {

    private static final String BINLOG_NAME_KEY = "Log_name";

    private final String host;

    private final int port;

    private final String user;

    private final String pass;

    public BinlogJournalValidator(String host, int port, String user, String pass) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.pass = pass;
    }

    public boolean check(String journalName) {
        return listJournals().contains(journalName);
    }

    /**
     * 查找Binlog日志列表
     *
     * @return log file list
     */
    public List<String> listJournals() {
        List<String> journalList = new ArrayList<>();
        MysqlConnection conn =
                new MysqlConnection(new InetSocketAddress(host, port), user, pass, (byte) 33, null);
        try {
            int logIndex = 0;
            conn.connect();
            ResultSetPacket resultSetPacket = conn.query("show binary logs");
            List<FieldPacket> fieldDescriptors = resultSetPacket.getFieldDescriptors();
            final int fieldSize = fieldDescriptors.size();
            for (int i = 0; i < fieldSize; i++) {
                if (fieldDescriptors.get(i).getName().equalsIgnoreCase(BINLOG_NAME_KEY)) {
                    logIndex = i;
                    break;
                }
            }
            List<String> fieldValues = resultSetPacket.getFieldValues();
            for (int i = logIndex; i < fieldValues.size(); i += fieldSize) {
                journalList.add(fieldValues.get(i));
            }
            log.info("collect journals: " + journalList);
        } catch (IOException e) {
            log.error("Error occurred: " + e.getMessage());
        } finally {
            try {
                conn.disconnect();
            } catch (IOException e) {
                log.error("Error occurred while disconnect mysql_connection: " + e.getMessage());
            }
        }
        return journalList;
    }
}
