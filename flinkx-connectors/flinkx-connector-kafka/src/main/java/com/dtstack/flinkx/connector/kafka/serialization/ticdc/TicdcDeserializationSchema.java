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

package com.dtstack.flinkx.connector.kafka.serialization.ticdc;

import com.dtstack.flinkx.cdc.DdlRowDataBuilder;
import com.dtstack.flinkx.cdc.EventType;
import com.dtstack.flinkx.connector.kafka.conf.KafkaConf;
import com.dtstack.flinkx.connector.kafka.source.DynamicKafkaDeserializationSchema;
import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.util.JsonUtil;
import com.pingcap.ticdc.cdc.KafkaMessage;
import com.pingcap.ticdc.cdc.TicdcEventData;
import com.pingcap.ticdc.cdc.TicdcEventDecoder;
import com.pingcap.ticdc.cdc.TicdcEventFilter;
import com.pingcap.ticdc.cdc.key.TicdcEventKey;
import com.pingcap.ticdc.cdc.value.TicdcEventBase;
import com.pingcap.ticdc.cdc.value.TicdcEventColumn;
import com.pingcap.ticdc.cdc.value.TicdcEventDDL;
import com.pingcap.ticdc.cdc.value.TicdcEventResolve;
import com.pingcap.ticdc.cdc.value.TicdcEventRowChange;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 用来解析ticdc数据
 *
 * @author tiezhu@dtstack.com
 * @since 2021/12/20 星期一
 */
public class TicdcDeserializationSchema extends DynamicKafkaDeserializationSchema {

    private static final long serialVersionUID = 1L;

    private static final String TYPE_STR = "u";

    protected static final String SCHEMA = "schema";
    protected static final String TABLE = "table";
    protected static final String TS = "ts";
    protected static final String OP_TIME = "opTime";
    protected static final String TYPE = "type";
    protected static final String BEFORE = "before";
    protected static final String AFTER = "after";

    private final KafkaConf kafkaConf;

    private transient TicdcEventFilter filter;

    private final AbstractCDCRowConverter<Pair<TicdcEventKey, TicdcEventRowChange>, String>
            converter;

    public TicdcDeserializationSchema(KafkaConf kafkaConf) {
        super(1, null, null, null, null, false, null, null, false);
        this.kafkaConf = kafkaConf;
        this.converter = new TicdcColumnConverter(kafkaConf.isPavingData(), kafkaConf.isSplit());
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        beforeOpen();
        this.filter = new TicdcEventFilter();
        LOG.info(
                "[{}] open successfully, \ninputSplit = {}, \n[{}]: \n{} ",
                this.getClass().getSimpleName(),
                "see other log",
                kafkaConf.getClass().getSimpleName(),
                JsonUtil.toPrintJson(kafkaConf));
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector) {
        // 如果数据是无用数据，那么跳过后续的解析操作
        if (uselessData(record.value())) {
            return;
        }

        try {
            beforeDeserialize(record);
            KafkaMessage kafkaMessage = extractKafkaMessage(record);

            TicdcEventDecoder decoder = new TicdcEventDecoder(kafkaMessage);
            while (decoder.hasNext()) {
                TicdcEventData ticdcEventData = decoder.next();

                TicdcEventKey eventKey = ticdcEventData.getTicdcEventKey();
                TicdcEventBase eventValue = ticdcEventData.getTicdcEventValue();

                if (eventValue instanceof TicdcEventResolve) {
                    filter.resolveEvent(eventValue.getKafkaPartition(), eventKey.getTs());
                    continue;
                }

                if (eventValue instanceof TicdcEventRowChange) {
                    // dml
                    Pair<TicdcEventKey, TicdcEventRowChange> ticdcEventPair =
                            Pair.of(eventKey, (TicdcEventRowChange) eventValue);
                    LinkedList<RowData> rowData = converter.toInternal(ticdcEventPair);
                    for (RowData rowDatum : rowData) {
                        collector.collect(rowDatum);
                    }
                    return;
                }

                if (eventValue instanceof TicdcEventDDL) {
                    // ddl
                    RowData fromDdl = fromDdl(eventKey, (TicdcEventDDL) eventValue);
                    collector.collect(fromDdl);
                    return;
                }
            }

        } catch (Exception e) {
            dirtyManager.collect(
                    new String(record.value(), StandardCharsets.UTF_8),
                    e,
                    null,
                    getRuntimeContext());
        }
    }

    /**
     * 解析ticdc ddl中的数据
     *
     * @param eventKey event key.
     * @param eventDDL event ddl.
     * @return flink ddl row-data from ticdc ddl.
     */
    private RowData fromDdl(TicdcEventKey eventKey, TicdcEventDDL eventDDL) {
        EventType eventType = TicdcEventTypeHelper.findDDL(eventDDL.getT());

        return DdlRowDataBuilder.builder()
                .setContent(eventDDL.getQ())
                .setType(eventType.name())
                .setDatabaseName(eventKey.getScm())
                .setTableName(eventKey.getTbl())
                .setLsn(String.valueOf(eventKey.getTs()))
                .build();
    }

    /**
     * 解析ticdc event row-change的数据
     *
     * @param eventKey event key.
     * @param rowChange event row-change.
     * @return flink dml row-data from ticdc row-change.
     */
    private RowData fromRowChange(TicdcEventKey eventKey, TicdcEventRowChange rowChange) {
        boolean checkOk =
                filter.check(eventKey.getTbl(), rowChange.getKafkaPartition(), eventKey.getTs());

        if (checkOk) {

            // 7: schema, table, ts, opTime，type, before, after
            ColumnRowData fromRowChange = new ColumnRowData(7);
            fromRowChange.addField(new StringColumn(eventKey.getScm()));
            fromRowChange.addHeader(SCHEMA);
            fromRowChange.addField(new StringColumn(eventKey.getTbl()));
            fromRowChange.addHeader(TABLE);
            fromRowChange.addField(new BigDecimalColumn(eventKey.getTs()));
            fromRowChange.addHeader(TS);
            fromRowChange.addField(new TimestampColumn(rowChange.getKafkaTimestamp()));
            fromRowChange.addHeader(OP_TIME);

            List<TicdcEventColumn> beforeColumns = rowChange.getOldColumns();
            List<TicdcEventColumn> afterColumns = rowChange.getColumns();

            if (TYPE_STR.equalsIgnoreCase(rowChange.getUpdateOrDelete())) {
                if (null == beforeColumns) {
                    // insert
                    List<AbstractBaseColumn> afterColumnList = new ArrayList<>(afterColumns.size());
                    List<String> afterHeaderList = new ArrayList<>(afterColumns.size());

                    Map<String, Object> afterValue = processColumnList(afterColumns);

                } else {
                    // update
                    List<AbstractBaseColumn> beforeColumnList =
                            new ArrayList<>(beforeColumns.size());
                    List<String> beforeHeaderList = new ArrayList<>(beforeColumns.size());
                    List<AbstractBaseColumn> afterColumnList = new ArrayList<>(afterColumns.size());
                    List<String> afterHeaderList = new ArrayList<>(afterColumns.size());
                }
            }
        }

        throw new IllegalArgumentException(
                "Fetch wrong row-change from ticdc. Event key: "
                        + eventKey
                        + ", event value: "
                        + rowChange);
    }

    private Map<String, Object> processColumnList(List<TicdcEventColumn> columnList) {
        Map<String, Object> map = Maps.newLinkedHashMapWithExpectedSize(columnList.size());
        for (TicdcEventColumn eventColumn : columnList) {
            map.put(eventColumn.getName(), eventColumn.getV());
        }
        return map;
    }

    /**
     * 从kafka record 中提取出ticdc kafka message{@link KafkaMessage}.
     *
     * @param record kafka record
     * @return kafka message
     */
    private KafkaMessage extractKafkaMessage(ConsumerRecord<byte[], byte[]> record) {
        KafkaMessage kafkaMessage = new KafkaMessage(record.key(), record.value());
        kafkaMessage.setOffset(record.offset());
        kafkaMessage.setPartition(record.partition());
        kafkaMessage.setTimestamp(record.timestamp());
        return kafkaMessage;
    }

    /**
     * 排除空的数据（因为ticdc会不断发送空的数据，保证对接数据源的可用性）
     *
     * @param record ticdc record
     * @return useless or not
     */
    private boolean uselessData(byte[] record) {
        boolean useless = true;
        for (byte b : record) {
            if (b != 0) {
                useless = false;
                break;
            }
        }
        return useless;
    }
}
