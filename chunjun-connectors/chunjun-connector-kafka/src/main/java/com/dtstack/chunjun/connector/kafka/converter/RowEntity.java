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

package com.dtstack.chunjun.connector.kafka.converter;

import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;

import org.apache.flink.types.RowKind;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RowEntity implements Serializable {
    private final RowKind rowKind;
    private final Map<String, Object> columnData;
    private final List<MetaDataColumn> metaDataColumn;
    private final String schema;
    private final String dataBase;
    private final String table;
    private final String topic;

    public RowEntity(
            RowKind rowKind,
            Map<String, Object> columnData,
            List<MetaDataColumn> metaDataColumn,
            String dataBase,
            String schema,
            String table,
            String topic) {
        this.rowKind = rowKind;
        this.columnData = columnData;
        this.metaDataColumn = metaDataColumn;
        this.dataBase = dataBase;
        this.schema = schema;
        this.table = table;
        this.topic = topic;

        if (rowKind == null || table == null) {
            throw new RuntimeException("rowkind or table not allow null");
        }
    }

    public RowKind getRowKind() {
        return rowKind;
    }

    public Map<String, Object> getColumnData() {
        return columnData;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getTopic() {
        return topic;
    }

    public List<MetaDataColumn> getMetaDataColumn() {
        return metaDataColumn;
    }

    public String getDataBase() {
        return dataBase;
    }

    public String getIdentifier() {
        List<String> identifier = new ArrayList<>();
        if (StringUtils.isNotBlank(dataBase)) {
            identifier.add(dataBase);
        }
        if (StringUtils.isNotBlank(schema)) {
            identifier.add(schema);
        }
        if (StringUtils.isNotBlank(table)) {
            identifier.add(table);
        }
        return String.join(".", identifier);
    }

    public static class MetaDataColumn implements Serializable {
        private final String key;
        private final IDeserializationConverter<Object, AbstractBaseColumn> convent;

        public MetaDataColumn(
                String key, IDeserializationConverter<Object, AbstractBaseColumn> convent) {
            this.key = key;
            this.convent = convent;
        }

        public String getKey() {
            return key;
        }

        public IDeserializationConverter<Object, AbstractBaseColumn> getConvent() {
            return convent;
        }
    }
}
