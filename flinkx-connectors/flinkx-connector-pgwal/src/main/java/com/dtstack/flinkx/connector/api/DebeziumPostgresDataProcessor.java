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

import com.dtstack.flinkx.element.ErrorMsgRowData;
import io.debezium.connector.common.SourceRecordWrapper;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DebeziumPostgresDataProcessor implements DataProcessor<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumPostgresDataProcessor.class);

    private ConnectorConsumer connectorConsumer;

    public DebeziumPostgresDataProcessor(Map<String, Object> param) {
        LOG.info("Init the class : {} , with param : {}" , this.getClass(), param);
    }

    @Override
    public void process(ServiceProcessor.Context entity) throws IOException, SQLException {
        List<SourceRecordWrapper> recordWrappers = entity.get("data", List.class);
        List<RowData> list = recordWrappers.stream().map(this::convertRecord).collect(Collectors.toList());
        if(!list.isEmpty()) {
            for (RowData rowData : list) {
                connectorConsumer.consumer(rowData);
            }
        }
    }

    private RowData convertRecord(SourceRecordWrapper record) {
        GenericRowData genericRowData = new GenericRowData(1);
        genericRowData.setField(0, buildChangeItem(record));
        return genericRowData;
    }

    private ChangeItem buildChangeItem(SourceRecordWrapper record) {
        return new ChangeItem(record);
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
