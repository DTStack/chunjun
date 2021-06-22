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

import io.debezium.connector.common.SourceRecordWrapper;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChangeItem {

    private final String schema;
    private final String op;
    private final List<ChangeEntry> changeKeys;
    private final List<ChangeEntry> oldEntries;
    private final List<ChangeEntry> newEntries;
    private final List<ChangeEntry> additional;

    public ChangeItem(SourceRecordWrapper record) {
        this.schema = buildSchema(record.value());
        this.op = buildOp(record.value());
        this.changeKeys = buildEntries(record.key(), record.keySchema());
        this.newEntries = buildEntries(castToMap(record.value()).get("after"), record.valueSchema().field("after").schema());
        this.oldEntries = buildEntries(castToMap(record.value()).get("before"), record.valueSchema().field("before").schema());
        this.additional = buildAdditional((Long) castToMap(record.value()).get("ts_ms"), castToMap(record.sourceOffset()));
    }

    private List<ChangeEntry> buildAdditional(Long tsMs, Map<String, Object> params) {
        List<ChangeEntry> changeEntries = new ArrayList<>();
        changeEntries.add(new ChangeEntry(tsMs, Schema.Type.INT64));
        changeEntries.add(new ChangeEntry(params.get("lsn"), Schema.Type.INT64));
        changeEntries.add(new ChangeEntry(params.get("tsId"), Schema.Type.INT64));
        return changeEntries;
    }

    private List<ChangeEntry> buildEntries(Object key, Schema schema) {
        if(key == null) {
            return null;
        }
        List<ChangeEntry> changeEntries = new ArrayList<>();
        Map<String, Object> map = castToMap(key);
        for (Field field: schema.fields()) {
            ChangeEntry entry = new ChangeEntry(map.get(field.name()), field.schema().type());
            changeEntries.add(entry);
        }
        return changeEntries;
    }

    private String buildOp(Object value) {
        Map<String, Object> sourceMap = castToMap(value);
        return (String) sourceMap.getOrDefault("op", "");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castSourceToMap(Object value) {
        Map<String, Object> valueMap = castToMap(value);
        return (Map<String, Object>) valueMap.getOrDefault("source", new HashMap<>());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castToMap(Object value) {
        assert value instanceof Map;
        return (Map<String, Object>) value;
    }

    private String buildSchema(Object value) {
        Map<String, Object> sourceMap = castSourceToMap(value);
        String dbName = (String) sourceMap.getOrDefault("db", "");
        String schema = (String) sourceMap.getOrDefault("schema", "");
        String table = (String) sourceMap.getOrDefault("table", "");
        return String.format("%s.%s.%s", dbName, schema, table);
    }

    public String getSchema() {
        return schema;
    }

    public String getOp() {
        return op;
    }

    public List<ChangeEntry> getChangeKeys() {
        return changeKeys;
    }

    public List<ChangeEntry> getOldEntries() {
        return oldEntries;
    }

    public List<ChangeEntry> getNewEntries() {
        return newEntries;
    }

    public List<ChangeEntry> getAdditional() {
        return additional;
    }

    @Override
    public String toString() {
        return "ChangeItem{" +
                "schema='" + schema + '\'' +
                ", op='" + op + '\'' +
                ", changeKeys=" + changeKeys +
                ", oldEntries=" + oldEntries +
                ", newEntries=" + newEntries +
                ", additional=" + additional +
                '}';
    }
}
