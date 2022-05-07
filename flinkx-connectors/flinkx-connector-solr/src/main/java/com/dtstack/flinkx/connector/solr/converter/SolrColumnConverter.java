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

package com.dtstack.flinkx.connector.solr.converter;

import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/20
 */
public class SolrColumnConverter
        extends AbstractRowConverter<SolrDocument, SolrDocument, SolrInputDocument, LogicalType> {
    protected List<SolrSerializationConverter> toExternalConverters;
    protected String[] fieldNames;

    public SolrColumnConverter(RowType rowType, String[] fieldNames) {
        super(rowType);
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
        ColumnRowData columnRowData = new ColumnRowData(toInternalConverters.size());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = input.getFieldValue(fieldNames[pos]);
            // when Solr collection is schemaless, it will return a ArrayList.
            if (field instanceof ArrayList) {
                field = ((ArrayList) field).get(0);
            }
            columnRowData.addField(
                    (AbstractBaseColumn) toInternalConverters.get(pos).deserialize(field));
        }
        return columnRowData;
    }

    @Override
    public SolrInputDocument toExternal(RowData rowData, SolrInputDocument solrInputDocument)
            throws Exception {
        for (int pos = 0; pos < rowData.getArity(); pos++) {
            toExternalConverters
                    .get(pos)
                    .serialize(rowData, pos, fieldNames[pos], solrInputDocument);
        }
        return solrInputDocument;
    }

    protected SolrSerializationConverter wrapIntoNullableSolrExternalConverter(
            SolrSerializationConverter solrSerializationConverter) {
        return (val, pos, name, solrInputDocument) -> {
            if (((ColumnRowData) val).getField(pos) == null) {
                solrInputDocument.setField(name, null);
            } else {
                solrSerializationConverter.serialize(val, pos, name, solrInputDocument);
            }
        };
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> new BigDecimalColumn(((Integer) val).byteValue());
            case SMALLINT:
            case INTEGER:
                return val -> new BigDecimalColumn(Integer.parseInt(String.valueOf(val)));
            case FLOAT:
                return val -> new BigDecimalColumn((Float) val);
            case DOUBLE:
                return val -> new BigDecimalColumn((Double) val);
            case BIGINT:
                return val -> new BigDecimalColumn((Long) val);
            case DECIMAL:
                return val -> new BigDecimalColumn((BigDecimal) val);
            case CHAR:
            case VARCHAR:
                return val -> new StringColumn((String) val);
            case DATE:
                return val -> new TimestampColumn(((java.util.Date) val));
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        new TimestampColumn(
                                Time.valueOf(String.valueOf(val)).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> new TimestampColumn((Timestamp) val);
            case BINARY:
            case VARBINARY:
                return val -> new BytesColumn((byte[]) val);
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    protected SolrSerializationConverter createSolrExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, pos, name, document) ->
                        document.setField(name, ((ColumnRowData) val).getField(pos).asBoolean());
            case TINYINT:
                return (val, pos, name, document) ->
                        document.setField(name, ((ColumnRowData) val).getField(pos).asBytes());
            case SMALLINT:
            case INTEGER:
                return (val, pos, name, document) ->
                        document.setField(name, ((ColumnRowData) val).getField(pos).asInt());
            case FLOAT:
                return (val, pos, name, document) ->
                        document.setField(name, ((ColumnRowData) val).getField(pos).asFloat());
            case DOUBLE:
                return (val, pos, name, document) ->
                        document.setField(name, ((ColumnRowData) val).getField(pos).asDouble());
            case BIGINT:
                return (val, pos, name, document) ->
                        document.setField(name, ((ColumnRowData) val).getField(pos).asLong());
            case DECIMAL:
                return (val, pos, name, document) ->
                        document.setField(name, ((ColumnRowData) val).getField(pos).asBigDecimal());
            case CHAR:
            case VARCHAR:
                return (val, pos, name, document) ->
                        document.setField(name, ((ColumnRowData) val).getField(pos).asString());
            case DATE:
                return (val, pos, name, document) ->
                        document.setField(
                                name,
                                Date.valueOf(
                                        ((ColumnRowData) val)
                                                .getField(pos)
                                                .asTimestamp()
                                                .toLocalDateTime()
                                                .toLocalDate()));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, pos, name, document) ->
                        document.setField(
                                name,
                                Time.valueOf(
                                        ((ColumnRowData) val)
                                                .getField(pos)
                                                .asTimestamp()
                                                .toLocalDateTime()
                                                .toLocalTime()));

            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, pos, name, document) ->
                        document.setField(name, ((ColumnRowData) val).getField(pos).asTimestamp());

            case BINARY:
            case VARBINARY:
                return (val, pos, name, document) ->
                        document.setField(name, ((ColumnRowData) val).getField(pos).asBytes());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
