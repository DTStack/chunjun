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

package com.dtstack.chunjun.connector.hbase.converter;

import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
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
import org.apache.flink.types.RowKind;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;

import static com.dtstack.chunjun.connector.hbase.config.HBaseConfigConstants.MULTI_VERSION_FIXED_COLUMN;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

public class HBaseSqlConverter
        extends AbstractRowConverter<Object, RowData, Mutation, LogicalType> {
    private static final long serialVersionUID = -8935215591844851238L;

    private static final int MIN_TIMESTAMP_PRECISION = 0;
    private static final int MAX_TIMESTAMP_PRECISION = 3;
    private static final int MIN_TIME_PRECISION = 0;
    private static final int MAX_TIME_PRECISION = 3;
    private HBaseTableSchema schema;
    private HBaseConfig hBaseConfig;
    private transient HBaseSerde serde;

    public HBaseSqlConverter(HBaseTableSchema schema, HBaseConfig hBaseConfig) {
        this.schema = schema;
        this.hBaseConfig = hBaseConfig;
    }

    public HBaseSqlConverter(RowType rowType, HBaseConfig hBaseConfig) {
        super(rowType);
        this.hBaseConfig = hBaseConfig;
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
        }
    }

    @Override
    public RowData toInternal(Object input) throws Exception {

        if (hBaseConfig.getMode().equalsIgnoreCase(MULTI_VERSION_FIXED_COLUMN)) {
            Cell current = (Cell) input;
            GenericRowData rowData = new GenericRowData(this.rowType.getFieldCount());
            String family = new String(CellUtil.cloneFamily(current));
            String qualifier = new String(CellUtil.cloneQualifier(current));
            String familyAndQualifier = family + ":" + qualifier;
            rowData.setField(
                    0, toInternalConverters.get(0).deserialize(CellUtil.cloneRow(current)));
            rowData.setField(
                    1, toInternalConverters.get(1).deserialize(familyAndQualifier.getBytes()));
            rowData.setField(2, current.getTimestamp());
            rowData.setField(
                    3, StringData.fromString(Bytes.toStringBinary(CellUtil.cloneValue(current))));
            return rowData;
        }

        if (serde == null) {
            this.serde = new HBaseSerde(schema, hBaseConfig);
        }

        return serde.convertToReusedRow((Result) input);
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                // reuse bytes
                return (IDeserializationConverter<byte[], StringData>) StringData::fromBytes;
            case BOOLEAN:
                return (IDeserializationConverter<byte[], Boolean>) Bytes::toBoolean;
            case BINARY:
            case VARBINARY:
                return value -> value;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                final int precision = decimalType.getPrecision();
                final int scale = decimalType.getScale();
                return (IDeserializationConverter<byte[], DecimalData>)
                        value -> {
                            BigDecimal decimal = Bytes.toBigDecimal(value);
                            return DecimalData.fromBigDecimal(decimal, precision, scale);
                        };
            case TINYINT:
                return (IDeserializationConverter<byte[], Byte>) value -> value[0];
            case SMALLINT:
                return (IDeserializationConverter<byte[], Short>) Bytes::toShort;
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
                return (IDeserializationConverter<byte[], Integer>) Bytes::toInt;
            case TIME_WITHOUT_TIME_ZONE:
                final int timePrecision = getPrecision(type);
                if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The precision %s of TIME type is out of the range [%s, %s] supported by "
                                            + "HBase connector",
                                    timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
                }
                return (IDeserializationConverter<byte[], Integer>) Bytes::toInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (IDeserializationConverter<byte[], Long>) Bytes::toLong;
            case FLOAT:
                return (IDeserializationConverter<byte[], Float>) Bytes::toFloat;
            case DOUBLE:
                return (IDeserializationConverter<byte[], Double>) Bytes::toDouble;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(type);
                if (timestampPrecision < MIN_TIMESTAMP_PRECISION
                        || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by "
                                            + "HBase connector",
                                    timestampPrecision,
                                    MIN_TIMESTAMP_PRECISION,
                                    MAX_TIMESTAMP_PRECISION));
                }
                return (IDeserializationConverter<byte[], TimestampData>)
                        value -> {
                            // TODO: support higher precision
                            long milliseconds = Bytes.toLong(value);
                            return TimestampData.fromEpochMillis(milliseconds);
                        };
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    @Override
    public Mutation toExternal(RowData rowData, Mutation output) throws Exception {
        if (serde == null) {
            this.serde = new HBaseSerde(schema, hBaseConfig);
        }
        RowKind kind = rowData.getRowKind();
        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            return serde.createPutMutation(rowData);
        } else {
            return serde.createDeleteMutation(rowData);
        }
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        return input;
    }
}
