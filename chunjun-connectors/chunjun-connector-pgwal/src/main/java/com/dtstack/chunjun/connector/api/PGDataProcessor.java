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

package com.dtstack.chunjun.connector.api;

import com.dtstack.chunjun.connector.pgwal.conf.PGWalConf;
import com.dtstack.chunjun.connector.pgwal.converter.PGWalColumnConverter;
import com.dtstack.chunjun.connector.pgwal.listener.PgWalListener;
import com.dtstack.chunjun.connector.pgwal.util.ChangeLog;
import com.dtstack.chunjun.connector.pgwal.util.PgDecoder;
import com.dtstack.chunjun.element.ErrorMsgRowData;

import org.apache.flink.table.data.RowData;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PGDataProcessor implements DataProcessor<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(PgWalListener.class);
    private static Gson gson = new Gson();
    private PGWalConf conf;

    private PgConnection conn;
    private PGReplicationStream stream;
    private PgDecoder decoder;
    private PGWalColumnConverter converter;

    private ByteBuffer buffer;

    private volatile boolean running;

    public PGDataProcessor(Map<String, Object> param) {
        LOG.info("PgWalListener start running.....");
    }

    @Override
    public List<RowData> process(ServiceProcessor.Context context) throws Exception {
        assert context.contains("data");
        ChangeLog changeLog = decoder.decode(context.get("data", ByteBuffer.class));
        if (StringUtils.isBlank(changeLog.getId())) {
            return new ArrayList<>();
        }
        String type = changeLog.getType().name().toLowerCase();
        if (!conf.getCat().contains(type)) {
            return new ArrayList<>();
        }
        if (!conf.getSimpleTables().contains(changeLog.getTable())) {
            return new ArrayList<>();
        }
        LOG.trace("table = {}", gson.toJson(changeLog));
        LinkedList<RowData> rowData = converter.toInternal(changeLog);
        return rowData;
    }

    @Override
    public boolean moreData() {
        return true;
    }

    @Override
    public List<RowData> processException(Exception e) {
        return Lists.newArrayList(new ErrorMsgRowData(e.getMessage()));
    }
}
