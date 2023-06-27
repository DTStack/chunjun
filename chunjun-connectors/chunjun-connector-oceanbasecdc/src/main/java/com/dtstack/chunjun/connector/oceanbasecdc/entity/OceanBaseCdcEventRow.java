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

package com.dtstack.chunjun.connector.oceanbasecdc.entity;

import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OceanBaseCdcEventRow implements Serializable {

    private static final long serialVersionUID = -8615379237827243622L;

    /** The database name in log message, would be in format 'tenant.db'. */
    private final String database;

    private final String table;
    private final DataMessage.Record.Type type;

    /** Timestamp in seconds. */
    private final long timestamp;

    private final Map<String, DataMessage.Record.Field> fieldsBefore;
    private final Map<String, DataMessage.Record.Field> fieldsAfter;

    public OceanBaseCdcEventRow(LogMessage logMessage) {
        this.database = logMessage.getDbName();
        this.table = logMessage.getTableName();
        this.type = logMessage.getOpt();
        this.timestamp = Long.parseLong(logMessage.getTimestamp());
        this.fieldsBefore = new HashMap<>();
        this.fieldsAfter = new HashMap<>();

        for (DataMessage.Record.Field field : logMessage.getFieldList()) {
            if (field.isPrev()) {
                fieldsBefore.put(field.getFieldname(), field);
            } else {
                fieldsAfter.put(field.getFieldname(), field);
            }
        }
    }

    /**
     * Convert field map to 'field name'->'field value (string)'
     *
     * @param fields Original field map.
     * @return Field value map.
     */
    @SuppressWarnings("rawtypes")
    public static Map toValueMap(Map<String, DataMessage.Record.Field> fields) {
        Map<String, String> map = new HashMap<>();
        fields.forEach(
                (name, field) -> {
                    if (field.getValue() == null) {
                        map.put(name, null);
                    } else {
                        if ("binary".equalsIgnoreCase(field.getEncoding())) {
                            map.put(name, field.getValue().toString("utf8"));
                        } else {
                            map.put(name, field.getValue().toString(field.getEncoding()));
                        }
                    }
                });
        return map;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public DataMessage.Record.Type getType() {
        return type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, DataMessage.Record.Field> getFieldsBefore() {
        return fieldsBefore;
    }

    public Map<String, DataMessage.Record.Field> getFieldsAfter() {
        return fieldsAfter;
    }
}
