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

package com.dtstack.chunjun.connector.solr.converter;

import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class SolrSqlConverter
        extends AbstractRowConverter<SolrDocument, SolrDocument, SolrInputDocument, LogicalType> {

    private static final long serialVersionUID = -1488018940395139854L;
    protected List<SolrSerializationConverter> toExternalConverters;
    protected String[] fieldNames;

    public SolrSqlConverter(RowType rowType, String[] fieldNames) {
        this.rowType = checkNotNull(rowType);
        this.fieldNames = fieldNames;
        this.fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        this.toInternalConverters = new ArrayList<>();
        this.toExternalConverters = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableSolrExternalConverter(
                            createSolrExternalConverter(fieldTypes[i])));
        }
    }

    @Override
    public RowData toInternal(SolrDocument input) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = input.getFieldValue(fieldNames[pos]);
            // When Solr collection is schemaless, it will return a ArrayList.
            if (field instanceof ArrayList) {
                field = ((ArrayList) field).get(0);
            }
            genericRowData.setField(pos, toInternalConverters.get(pos).deserialize(field));
        }
        return genericRowData;
    }

    @Override
    public SolrInputDocument toExternal(RowData rowData, SolrInputDocument solrInputDocument) {
        for (int pos = 0; pos < fieldTypes.length; pos++) {
            toExternalConverters
                    .get(pos)
                    .serialize(rowData, pos, fieldNames[pos], solrInputDocument);
        }
        return solrInputDocument;
    }

    @Override
    protected IDeserializationConverter wrapIntoNullableInternalConverter(
            IDeserializationConverter deserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return deserializationConverter.deserialize(val);
            }
        };
    }

    protected SolrSerializationConverter wrapIntoNullableSolrExternalConverter(
            SolrSerializationConverter solrSerializationConverter) {
        return (val, pos, name, solrInputDocument) -> {
            if (val == null) {
                solrInputDocument.setField(name, null);
            } else {
                solrSerializationConverter.serialize(val, pos, name, solrInputDocument);
            }
        };
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case INTEGER:
            case BIGINT:
                return val -> val;
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                // Converter for small type that casts value to int and then return short value,
                // since
                // JDBC 1.0 use int type for small values.
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val ->
                        (int) ((Date.valueOf(String.valueOf(val))).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        (int)
                                ((Time.valueOf(String.valueOf(val))).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromTimestamp((Timestamp) val);
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
                return val -> (byte[]) val;
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    protected SolrSerializationConverter createSolrExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, pos, name, document) -> document.setField(name, val.getBoolean(pos));
            case TINYINT:
                return (val, pos, name, document) -> document.setField(name, val.getByte(pos));
            case SMALLINT:
                return (val, pos, name, document) -> document.setField(name, val.getShort(pos));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, pos, name, document) -> document.setField(name, val.getInt(pos));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (val, pos, name, document) -> document.setField(name, val.getLong(pos));
            case FLOAT:
                return (val, pos, name, document) -> document.setField(name, val.getFloat(pos));
            case DOUBLE:
                return (val, pos, name, document) -> document.setField(name, val.getDouble(pos));
            case CHAR:
            case VARCHAR:
                return (val, pos, name, document) ->
                        document.setField(name, val.getString(pos).toString());
            case BINARY:
            case VARBINARY:
                return (val, pos, name, document) -> document.setField(name, val.getBinary(pos));
            case DATE:
                return (val, pos, name, document) ->
                        document.setField(
                                name, Date.valueOf(LocalDate.ofEpochDay(val.getInt(pos))));

            case TIME_WITHOUT_TIME_ZONE:
                return (val, pos, name, document) ->
                        document.setField(
                                name,
                                Time.valueOf(LocalTime.ofNanoOfDay(val.getInt(pos) * 1_000_000L)));

            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, pos, name, document) ->
                        document.setField(
                                name, val.getTimestamp(pos, timestampPrecision).toTimestamp());
            case DECIMAL:
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
