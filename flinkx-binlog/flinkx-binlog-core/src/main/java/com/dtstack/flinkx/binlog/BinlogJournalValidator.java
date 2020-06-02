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
package com.dtstack.flinkx.binlog;


import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @author toutian
 */
public class BinlogJournalValidator {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogJournalValidator.class);

    private String host;

    private int port;

    private String user;

    private String pass;

    public BinlogJournalValidator(String host, int port, String user, String pass) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.pass = pass;
    }

    public boolean check(String journalName) {
        return listJournals().contains(journalName);
    }

    public List<String> listJournals() {
        List<String> journalList = new ArrayList<>();
        MysqlConnection conn = new MysqlConnection(new InetSocketAddress(host, port), user, pass, (byte) 33, null);
        try {
            conn.connect();
            ResultSetPacket resultSetPacket = conn.query("show binary logs");
            List<String> fieldValues = resultSetPacket.getFieldValues();
            for(int i = 0; i < fieldValues.size(); ++i) {
                if(i % 2 == 0) {
                    journalList.add(fieldValues.get(i));
                }
            }
            LOG.info("collect journals: " + journalList);
        } catch (IOException e) {
            LOG.error("Error occured: " + e.getMessage());
        } finally {
            if(conn != null) {
                try {
                    conn.disconnect();
                } catch (IOException e) {
                    LOG.error("Error occured while disconnect mysqlconnection: " + e.getMessage());
                }
            }
        }
        return journalList;
    }

}
