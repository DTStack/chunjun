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

package com.dtstack.flinkx.connector.oracle.converter;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import oracle.sql.TIMESTAMP;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;


/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleColumnConverter
        extends JdbcColumnConverter {

    public OracleColumnConverter(RowType rowType) {
        super(rowType);
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
                return val -> new BigDecimalColumn((Integer) val);
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
                return val -> {
                    if (type instanceof ClobType) {
                        oracle.sql.CLOB clob = (oracle.sql.CLOB) val;
                        try (BufferedReader bf = new BufferedReader(clob.getCharacterStream())){
                            StringBuilder stringBuilder = new StringBuilder();
                            String next, line = bf.readLine();
                            for (boolean last = (line == null); !last; line = next) {
                                last = ((next = bf.readLine()) == null);
                                if (last) {
                                    stringBuilder.append(line);
                                } else {
                                    stringBuilder.append(line).append("\n");
                                }
                            }
                            return new StringColumn(stringBuilder.toString());
                        }
                    } else {
                        return new StringColumn((String) val);
                    }
                };
            case DATE:
                return val -> new TimestampColumn((Timestamp) val);
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        new BigDecimalColumn(
                                Time.valueOf(String.valueOf(val)).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> new TimestampColumn(((TIMESTAMP) val).timestampValue());
            case BINARY:
            case VARBINARY:
                return val -> {
                    if (type instanceof BlobType) {
                        oracle.sql.BLOB blob = (oracle.sql.BLOB) val;
                        byte[] bytes = toByteArray(blob);
                        return new BytesColumn(bytes);
                    } else {
                        return new BytesColumn((byte[]) val);
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement> createExternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement) ->
                        statement.setBoolean(
                                index, ((ColumnRowData) val).getField(index).asBoolean());
            case TINYINT:
                return (val, index, statement) -> statement.setByte(index, val.getByte(index));
            case SMALLINT:
            case INTEGER:
                return (val, index, statement) ->
                        statement.setInt(index, ((ColumnRowData) val).getField(index).asInt());
            case FLOAT:
                return (val, index, statement) ->
                        statement.setFloat(index, ((ColumnRowData) val).getField(index).asFloat());
            case DOUBLE:
                return (val, index, statement) ->
                        statement.setDouble(
                                index, ((ColumnRowData) val).getField(index).asDouble());

            case BIGINT:
                return (val, index, statement) ->
                        statement.setLong(index, ((ColumnRowData) val).getField(index).asLong());
            case DECIMAL:
                return (val, index, statement) ->
                        statement.setBigDecimal(
                                index, ((ColumnRowData) val).getField(index).asBigDecimal());
            case CHAR:
            case VARCHAR:
                return (val, index, statement) -> {
                    if(type instanceof ClobType){
                        try (StringReader reader = new StringReader(((ColumnRowData) val).getField(index).asString())) {
                            statement.setClob(index, reader);
                        }
                    }else {
                        statement.setString(
                                index, ((ColumnRowData) val).getField(index).asString());
                    }
                };
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTime(
                                index,
                                Time.valueOf(
                                        LocalTime.ofNanoOfDay(
                                                ((ColumnRowData) val).getField(index).asInt()
                                                        * 1_000_000L)));
            case DATE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, ((ColumnRowData) val).getField(index).asTimestamp());

            case BINARY:
            case VARBINARY:
                return (val, index, statement) -> {
                    if(type instanceof BlobType){
                        try(InputStream is = new ByteArrayInputStream(val.getBinary(index))) {
                            statement.setBlob(index, is);
                        }
                    }else {
                        statement.setBytes(index, val.getBinary(index));
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private byte[] toByteArray(oracle.sql.BLOB fromBlob) throws SQLException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        InputStream is = fromBlob.getBinaryStream();
        try {
            byte[] buf = new byte[4000];
            for (; ; ) {
                int dataSize = is.read(buf);
                if (dataSize == -1) {
                    break;
                }
                baos.write(buf, 0, dataSize);
            }
            return baos.toByteArray();
        } finally {
            baos.close();
            if (is != null) {
                is.close();
            }
        }
    }

}
