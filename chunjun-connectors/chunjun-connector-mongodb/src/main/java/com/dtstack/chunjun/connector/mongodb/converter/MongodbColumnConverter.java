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

import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;

import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;

import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Ada Wong
 * @program chunjun
 * @create 2021/06/21
 */
public class MongodbColumnConverter
        extends AbstractRowConverter<Document, Document, Document, LogicalType> {

    private final List<MongoDeserializationConverter> toInternalConverters;
    private final List<MongoSerializationConverter> toExternalConverters;
    private final String[] fieldNames;

    public MongodbColumnConverter(RowType rowType, ChunJunCommonConf commonConf) {
        super(rowType, commonConf);
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
        List<FieldConf> fieldList = commonConf.getColumn();
        ColumnRowData result = new ColumnRowData(fieldList.size());
        int convertIndex = 0;
        for (FieldConf fieldConf : fieldList) {
            AbstractBaseColumn baseColumn = null;
            if ("flinkBson".equalsIgnoreCase(fieldConf.getName())) {
                Object field = JSON.serialize(document);
                baseColumn =
                        (AbstractBaseColumn)
                                toInternalConverters.get(convertIndex).deserialize(field);
                convertIndex++;
            } else if (StringUtils.isNullOrWhitespaceOnly(fieldConf.getValue())) {
                Object field = JSON.serialize(document.get(fieldConf.getName()));
                baseColumn =
                        (AbstractBaseColumn)
                                toInternalConverters.get(convertIndex).deserialize(field);
                convertIndex++;
            }
            result.addField(assembleFieldProps(fieldConf, baseColumn));
        }
        return result;
    }

    @Override
    public Document toExternal(RowData rowData, Document document) {
        for (int pos = 0; pos < rowData.getArity(); pos++) {
            toExternalConverters.get(pos).serialize(rowData, pos, fieldNames[pos], document);
        }
        return document;
    }

    private MongoDeserializationConverter createMongoInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> new BigDecimalColumn(((Integer) val).byteValue());
            case SMALLINT:
            case INTEGER:
                return val -> new BigDecimalColumn((Integer) val);
            case FLOAT:
                return val -> new BigDecimalColumn((Float) val);
            case DOUBLE:
                return val -> new BigDecimalColumn((Double) val);
            case BIGINT:
                return val -> new BigDecimalColumn((Long) val);
            case DECIMAL:
                return val -> new BigDecimalColumn(((Decimal128) val).bigDecimalValue());
            case CHAR:
            case VARCHAR:
                return val -> new StringColumn((String) val);
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
