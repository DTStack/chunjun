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

package com.dtstack.chunjun.connector.pgwal.listener;

import com.dtstack.chunjun.connector.pgwal.conf.PGWalConf;
import com.dtstack.chunjun.connector.pgwal.converter.PGWalColumnConverter;
import com.dtstack.chunjun.connector.pgwal.inputformat.PGWalInputFormat;
import com.dtstack.chunjun.connector.pgwal.util.ChangeLog;
import com.dtstack.chunjun.connector.pgwal.util.PGUtil;
import com.dtstack.chunjun.connector.pgwal.util.PgDecoder;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.RetryUtil;

import org.apache.flink.table.data.RowData;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/** */
public class PgWalListener implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(PgWalListener.class);
    private static Gson gson = new Gson();
    private final PGWalConf conf;

    private PGWalInputFormat format;
    private PgConnection conn;
    private PGReplicationStream stream;
    private PgDecoder decoder;
    private PGWalColumnConverter converter;

    public PgWalListener(PGWalInputFormat format) throws SQLException {
        this.format = format;
        this.conf = format.getConf();
    }

    public void init() throws Exception {
        this.conn =
                RetryUtil.executeWithRetry(
                        () ->
                                (PgConnection)
                                        PGUtil.getConnection(
                                                conf.jdbcUrl, conf.username, conf.password),
                        3,
                        2000,
                        true);
        converter = new PGWalColumnConverter(conf.pavingData, conf.pavingData);
        decoder = new PgDecoder(PGUtil.queryTypes(conn), conf);
        ChainedLogicalStreamBuilder builder =
                conn.getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName(conf.getSlotName())
                        // 协议版本。当前仅支持版本1
                        .withSlotOption("proto_version", "1")
                        // 逗号分隔的要订阅的发布名称列表（接收更改）。 单个发布名称被视为标准对象名称，并可根据需要引用
                        .withSlotOption("publication_names", PGUtil.PUBLICATION_NAME)
                        .withStatusInterval(conf.getStatusInterval(), TimeUnit.SECONDS);
        long lsn = format.getStartLsn();
        if (lsn != 0) {
            builder.withStartPosition(LogSequenceNumber.valueOf(lsn));
        }
        stream = builder.start();
        TimeUnit.SECONDS.sleep(1);
        stream.forceUpdateStatus();
        LOG.info("init PGReplicationStream successfully...");
    }

    @Override
    public void run() {
        LOG.info("PgWalListener start running.....");
        try {
            init();
            while (format.isRunning()) {
                ByteBuffer buffer = stream.readPending();
                if (buffer == null) {
                    continue;
                }
                ChangeLog changeLog = decoder.decode(buffer);
                if (StringUtils.isBlank(changeLog.getId())) {
                    continue;
                }
                String type = changeLog.getType().name().toLowerCase();
                if (!conf.getCat().contains(type)) {
                    continue;
                }
                if (!conf.getSimpleTables().contains(changeLog.getTable())) {
                    continue;
                }
                LOG.trace("table = {}", gson.toJson(changeLog));
                LinkedList<RowData> rowData = converter.toInternal(changeLog);
                format.appendResult(rowData);
            }
        } catch (Exception e) {
            String errorMessage = ExceptionUtil.getErrorMessage(e);
            LOG.error(errorMessage);
            format.processException(e);
        }
    }
}
