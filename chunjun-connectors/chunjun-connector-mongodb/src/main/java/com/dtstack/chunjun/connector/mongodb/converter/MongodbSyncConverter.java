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

package com.dtstack.chunjun.connector.mongodb.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;

import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongodbSyncConverter
        extends AbstractRowConverter<Document, Document, Document, LogicalType> {

    private static final long serialVersionUID = -7644004277309401498L;

    private final List<MongoDeserializationConverter> toInternalConverters;
    private final List<MongoSerializationConverter> toExternalConverters;
    private final String[] fieldNames;

    public MongodbSyncConverter(RowType rowType, CommonConfig commonConfig) {
        super(rowType, commonConfig);
        this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
        toInternalConverters = new ArrayList<>();
        toExternalConverters = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createMongoInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableMongodbExternalConverter(
                            createMongodbExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
    }

    protected MongoDeserializationConverter wrapIntoNullableInternalConverter(
            MongoDeserializationConverter deserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return deserializationConverter.deserialize(val);
            }
        };
    }

    protected MongoSerializationConverter wrapIntoNullableMongodbExternalConverter(
            MongoSerializationConverter serializationConverter, LogicalType type) {
        return (val, index, key, document) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                document.append(key, null);
            } else {
                serializationConverter.serialize(val, index, key, document);
            }
        };
    }

    @Override
    public RowData toInternal(Document document) {
        List<FieldConfig> fieldList = commonConfig.getColumn();
        ColumnRowData result = new ColumnRowData(fieldList.size());
        for (int i = 0; i < fieldList.size(); i++) {
            AbstractBaseColumn baseColumn = null;
            if (StringUtils.isNullOrWhitespaceOnly(fieldList.get(i).getValue())) {
                Object field = document.get(fieldList.get(i).getName());
                baseColumn = (AbstractBaseColumn) toInternalConverters.get(i).deserialize(field);
            }
            result.addField(assembleFieldProps(fieldList.get(i), baseColumn));
        }
        return result;
    }

    @Override
    public Document toExternal(RowData rowData, Document document) {
        for (int pos = 0; pos < fieldTypes.length; pos++) {
            toExternalConverters.get(pos).serialize(rowData, pos, fieldNames[pos], document);
        }
        return document;
    }

    private MongoDeserializationConverter createMongoInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> new ByteColumn(((Integer) val).byteValue());
            case SMALLINT:
                return val -> new ShortColumn(((Integer) val).shortValue());
            case INTEGER:
                return val -> new IntColumn((Integer) val);
            case FLOAT:
                return val -> new FloatColumn((Float) val);
            case DOUBLE:
                return val -> new DoubleColumn((Double) val);
            case BIGINT:
                return val -> new LongColumn((Long) val);
            case DECIMAL:
                return val -> new BigDecimalColumn(((Decimal128) val).bigDecimalValue());
            case CHAR:
            case VARCHAR:
                return val -> {
                    if (val instanceof List || val instanceof Map) {
                        return new StringColumn(GsonUtil.GSON.toJson(val));
                    } else {
                        return new StringColumn(val.toString());
                    }
                };
            case INTERVAL_YEAR_MONTH:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> new TimestampColumn((java.util.Date) val, 0);
            case BINARY:
            case VARBINARY:
                return val -> new BytesColumn(((Binary) val).getData());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private MongoSerializationConverter createMongodbExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, key, document) ->
                        document.append(key, ((ColumnRowData) val).getField(index).asBoolean());
            case TINYINT:
                return (val, index, key, document) -> document.append(key, val.getByte(index));
            case SMALLINT:
            case INTEGER:
                return (val, index, key, document) ->
                        document.append(key, ((ColumnRowData) val).getField(index).asInt());
            case FLOAT:
                return (val, index, key, document) ->
                        document.append(key, ((ColumnRowData) val).getField(index).asFloat());
            case DOUBLE:
                return (val, index, key, document) ->
                        document.append(key, ((ColumnRowData) val).getField(index).asDouble());
            case BIGINT:
                return (val, index, key, document) ->
                        document.append(key, ((ColumnRowData) val).getField(index).asLong());
            case DECIMAL:
                return (val, index, key, document) ->
                        document.append(key, ((ColumnRowData) val).getField(index).asBigDecimal());
            case CHAR:
            case VARCHAR:
                return (val, index, key, document) ->
                        document.append(key, ((ColumnRowData) val).getField(index).asString());
            case INTERVAL_YEAR_MONTH:
                return (val, index, key, document) ->
                        document.append(
                                key,
                                ((ColumnRowData) val)
                                        .getField(index)
                                        .asTimestamp()
                                        .toLocalDateTime()
                                        .toLocalDate()
                                        .getYear());
            case DATE:
                return (val, index, key, document) ->
                        document.append(
                                key,
                                Date.valueOf(
                                        ((ColumnRowData) val)
                                                .getField(index)
                                                .asTimestamp()
                                                .toLocalDateTime()
                                                .toLocalDate()));

            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, key, document) ->
                        document.append(
                                key,
                                Time.valueOf(
                                        ((ColumnRowData) val)
                                                .getField(index)
                                                .asTimestamp()
                                                .toLocalDateTime()
                                                .toLocalTime()));

            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, key, document) ->
                        document.append(key, ((ColumnRowData) val).getField(index).asTimestamp());
            case BINARY:
            case VARBINARY:
                return (val, index, key, document) ->
                        document.append(key, ((ColumnRowData) val).getField(index).asBytes());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
