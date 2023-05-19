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
package com.dtstack.chunjun.connector.db2.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcSyncConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.ibm.db2.jcc.am.c9;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

public class Db2SyncConverter extends JdbcSyncConverter {

    private static final long serialVersionUID = -3154012344694748358L;

    public Db2SyncConverter(RowType rowType, CommonConfig commonConfig) {
        super(rowType, commonConfig);
    }

    /**
     * override reason: blob in db2 need use getBytes.
     *
     * @param type
     * @return
     */
    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> new ByteColumn((Byte) val);
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
                return val -> new BigDecimalColumn((BigDecimal) val);
            case CHAR:
            case VARCHAR:
                return val -> {
                    // db2 clob type
                    if (val instanceof c9) {
                        Clob clob = (Clob) val;
                        return new StringColumn(convertClob(clob));
                    }
                    return new StringColumn((String) val);
                };
            case DATE:
                return val -> new SqlDateColumn((Date) val);
            case TIME_WITHOUT_TIME_ZONE:
                return val -> new TimeColumn((Time) val);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> new TimestampColumn((Timestamp) val);
            case BINARY:
            case VARBINARY:
                return val -> {
                    Blob blob = (Blob) val;
                    int length;
                    try {
                        length = (int) blob.length();
                        return new BytesColumn(blob.getBytes(1, length));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public static String convertClob(Clob clob) throws SQLException, IOException {
        StringBuilder builder = new StringBuilder();
        try (Reader r = clob.getCharacterStream()) {
            int ch;
            while ((ch = r.read()) != -1) {
                builder.append((char) ch);
            }
        }
        clob.free();
        return builder.toString();
    }
}
