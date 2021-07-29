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

package com.dtstack.flinkx.pgwal.listener;

import com.dtstack.flinkx.pgwal.PgDecoder;
import com.dtstack.flinkx.pgwal.PgWalUtil;
import com.dtstack.flinkx.pgwal.Table;
import com.dtstack.flinkx.pgwal.format.PgWalInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Date: 2019/12/14
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PgWalListener implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(PgWalListener.class);
    private static Gson gson = new Gson();

    private PgWalInputFormat format;
    private PgConnection conn;
    private Set<String> tableSet;
    private Set<String> cat;
    private boolean pavingData;

    private PGReplicationStream stream;
    private PgDecoder decoder;

    public PgWalListener(PgWalInputFormat format) {
        this.format = format;
        this.conn = format.getConn();
        this.tableSet = new HashSet<>(format.getTableList());
        this.cat = new HashSet<>();
        for (String type : format.getCat().split(",")) {
            cat.add(type.toLowerCase());
        }
        this.pavingData = format.isPavingData();
    }

    public void init() throws Exception{
        decoder = new PgDecoder(PgWalUtil.queryTypes(conn));
        ChainedLogicalStreamBuilder builder = conn.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(format.getSlotName())
                //协议版本。当前仅支持版本1
                .withSlotOption("proto_version", "1")
                //逗号分隔的要订阅的发布名称列表（接收更改）。 单个发布名称被视为标准对象名称，并可根据需要引用
                .withSlotOption("publication_names", PgWalUtil.PUBLICATION_NAME)
                .withStatusInterval(format.getStatusInterval(), TimeUnit.MILLISECONDS);
        long lsn = format.getStartLsn();
        if(lsn != 0){
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
                Table table = decoder.decode(buffer);
                if(StringUtils.isBlank(table.getId())){
                    continue;
                }
                String type = table.getType().name().toLowerCase();
                if(!cat.contains(type)){
                    continue;
                }
                if(!tableSet.contains(table.getId())){
                    continue;
                }
                LOG.trace("table = {}",gson.toJson(table));
                Map<String, Object> map = new LinkedHashMap<>();
                map.put("type", type);
                map.put("schema", table.getSchema());
                map.put("table", table.getTable());
                map.put("lsn", table.getCurrentLsn());
                map.put("ts", table.getTs());
                map.put("ingestion", System.nanoTime());
                if(pavingData){
                    int i = 0;
                    for (MetaColumn column : table.getColumnList()) {
                        map.put("before_" + column.getName(), table.getOldData()[i]);
                        map.put("after_" + column.getName(), table.getNewData()[i]);
                        i++;
                    }
                }else {
                    Map<String, Object> before = new LinkedHashMap<>();
                    Map<String, Object> after = new LinkedHashMap<>();
                    int i = 0;
                    for (MetaColumn column : table.getColumnList()) {
                        before.put(column.getName(), table.getOldData()[i]);
                        after.put(column.getName(), table.getNewData()[i]);
                        i++;
                    }
                    map.put("before", before);
                    map.put("after", after);
                }
                format.processEvent(map);
            }
        }catch (Exception e){
            String errorMessage = ExceptionUtil.getErrorMessage(e);
            LOG.error(errorMessage);
            format.processEvent(Collections.singletonMap("e", errorMessage));

        }
    }
}
