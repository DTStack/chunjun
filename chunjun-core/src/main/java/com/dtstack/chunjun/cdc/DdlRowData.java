/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.cdc;

import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static com.dtstack.chunjun.element.ClassSizeUtil.getStringSize;

public class DdlRowData implements RowData, Serializable {

    private static final long serialVersionUID = -6694422834471791214L;

    private final String[] headers;
    private final String[] ddlInfos;
    private RowKind rowKind;
    private int byteSize = 0;

    public DdlRowData(String[] headers) {
        this.headers = headers;
        this.ddlInfos = new String[headers.length];
        for (int i = 0; i < headers.length; i++) {
            byteSize += getStringSize(headers[i]);
        }
    }

    public DdlRowData(String[] headers, String[] ddlInfos, int byteSize) {
        this.headers = headers;
        this.ddlInfos = ddlInfos;
        this.byteSize = byteSize;
    }

    public void setDdlInfo(int index, String info) {
        ddlInfos[index] = info;
        byteSize += getStringSize(info);
    }

    public void setDdlInfo(String headerName, String info) {
        for (int index = 0; index < headers.length; index++) {
            if (headers[index].equals(headerName)) {
                ddlInfos[index] = info;
                byteSize += getStringSize(info);
                break;
            }
        }
    }

    public String getInfo(int pos) {
        return ddlInfos[pos];
    }

    @Override
    public int getArity() {
        return ddlInfos.length;
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        this.rowKind = rowKind;
    }

    @Override
    public boolean isNullAt(int pos) {
        return this.ddlInfos[pos] == null;
    }

    @Override
    public boolean getBoolean(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to boolean");
    }

    @Override
    public byte getByte(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to byte");
    }

    @Override
    public short getShort(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to short");
    }

    @Override
    public int getInt(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to int");
    }

    @Override
    public long getLong(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to long");
    }

    @Override
    public float getFloat(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to float");
    }

    @Override
    public double getDouble(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to double");
    }

    @Override
    public StringData getString(int i) {
        return BinaryStringData.fromString(ddlInfos[i]);
    }

    @Override
    public DecimalData getDecimal(int i, int i1, int i2) {
        throw new UnsupportedOperationException("DDL RowData can't transform to decimal");
    }

    @Override
    public TimestampData getTimestamp(int i, int i1) {
        throw new UnsupportedOperationException("DDL RowData can't transform to timestamp");
    }

    @Override
    public <T> RawValueData<T> getRawValue(int i) {
        throw new UnsupportedOperationException("DDL RowData can't transform to RawValueData");
    }

    @Override
    public byte[] getBinary(int i) {
        return ddlInfos[i].getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public ArrayData getArray(int i) {
        return new GenericArrayData(ddlInfos);
    }

    @Override
    public MapData getMap(int i) {
        final Map<String, Object> ddlMap = new HashMap<>();
        for (int j = 0; j < ddlInfos.length; j++) {
            ddlMap.put(headers[j], ddlInfos[j]);
        }
        return new GenericMapData(ddlMap);
    }

    @Override
    public RowData getRow(int i, int i1) {
        throw new UnsupportedOperationException("DDL RowData can't transform to Row");
    }

    public String getSql() {
        for (int i = 0; i < headers.length; i++) {
            if ("content".equalsIgnoreCase(headers[i])) {
                return ddlInfos[i];
            }
        }
        throw new IllegalArgumentException("Can not find content from DDL RowData!");
    }

    public TableIdentifier getTableIdentifier() {
        String dataBase = null;
        String schema = null;
        String table = null;
        for (int i = 0; i < headers.length; i++) {

            if ("database".equalsIgnoreCase(headers[i])) {
                dataBase = ddlInfos[i];
                continue;
            }

            if ("schema".equalsIgnoreCase(headers[i])) {
                schema = ddlInfos[i];
                continue;
            }

            if ("table".equalsIgnoreCase(headers[i])) {
                table = ddlInfos[i];
            }
        }
        return new TableIdentifier(dataBase, schema, table);
    }

    public EventType getType() {
        for (int i = 0; i < headers.length; i++) {
            if ("type".equalsIgnoreCase(headers[i])) {
                if (EventType.contains(ddlInfos[i])) {
                    return EventType.valueOf(ddlInfos[i]);
                } else {
                    return EventType.UNKNOWN;
                }
            }
        }
        throw new IllegalArgumentException("Can not find type from DDL RowData!");
    }

    public boolean isSnapShot() {
        for (int i = 0; i < headers.length; i++) {
            if ("snapshot".equalsIgnoreCase(headers[i])) {
                return "true".equalsIgnoreCase(ddlInfos[i]);
            }
        }
        throw new IllegalArgumentException("Can not find type from DDL RowData!");
    }

    public String getLsn() {
        for (int i = 0; i < headers.length; i++) {
            if ("lsn".equalsIgnoreCase(headers[i])) {
                return ddlInfos[i];
            }
        }
        throw new IllegalArgumentException("Can not find lsn from DDL RowData!");
    }

    public Integer getLsnSequence() {
        for (int i = 0; i < headers.length; i++) {
            if ("lsn_sequence".equalsIgnoreCase(headers[i])) {
                return Integer.valueOf(ddlInfos[i]);
            }
        }
        return 0;
    }

    public DdlRowData copy() {
        DdlRowData ddlRowData = new DdlRowData(headers);
        for (int i = 0; i < headers.length; i++) {
            ddlRowData.setDdlInfo(i, this.getInfo(i));
        }

        ddlRowData.setRowKind(rowKind);

        return ddlRowData;
    }

    public String[] getHeaders() {
        return headers;
    }

    public int getByteSize() {
        return byteSize;
    }
}
