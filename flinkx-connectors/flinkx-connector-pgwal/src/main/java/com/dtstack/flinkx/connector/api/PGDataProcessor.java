/*
 *    Copyright 2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.dtstack.flinkx.connector.api;

import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.connector.pgwal.conf.PGWalConf;
import com.dtstack.flinkx.connector.pgwal.converter.PGWalColumnConverter;
import com.dtstack.flinkx.connector.pgwal.listener.PgWalListener;
import com.dtstack.flinkx.connector.pgwal.util.ChangeLog;
import com.dtstack.flinkx.connector.pgwal.util.PgDecoder;
import com.dtstack.flinkx.element.ErrorMsgRowData;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
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
    private ConnectorConsumer connectorConsumer;

    private volatile boolean running;

    public PGDataProcessor(Map<String, Object> param) {
        LOG.info("PgWalListener start running.....");
    }

    @Override
    public void process(ServiceProcessor.Context context) throws SQLException {
        assert context.contains("data");
        ChangeLog changeLog = decoder.decode(context.get("data", ByteBuffer.class));
        if (StringUtils.isBlank(changeLog.getId())) {
            return;
        }
        String type = changeLog.getType().name().toLowerCase();
        if (!conf.getCat().contains(type)) {
            return;
        }
        if (!conf.getSimpleTables().contains(changeLog.getTable())) {
            return;
        }
        LOG.trace("table = {}", gson.toJson(changeLog));
        List<RowData> rowData = converter.toInternal(changeLog);
        if(rowData != null && !rowData.isEmpty()) {
            for (RowData data : rowData) {
                connectorConsumer.consumer(data);
            }
        }
    }

    @Override
    public boolean moreData() {
        return true;
    }

    @Override
    public void processException(Exception e) {
        connectorConsumer.consumer(new ErrorMsgRowData(e.getMessage()));
    }

    @Override
    public void setConnectorConsumer(ConnectorConsumer connectorConsumer) {
        this.connectorConsumer = connectorConsumer;
    }
}
