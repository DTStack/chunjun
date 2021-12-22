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
package com.dtstack.flinkx.element;

import com.dtstack.flinkx.element.column.NullColumn;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Date: 2021/04/26 Company: www.dtstack.com
 *
 * @author tudou
 */
public final class ColumnRowData implements RowData, Serializable {

    private static final long serialVersionUID = 1L;
    private final List<AbstractBaseColumn> columnList;
    private Map<String, Integer> header;
    private Set<String> extHeader = new HashSet<>();

    private RowKind kind;

    public ColumnRowData(RowKind kind, int arity) {
        this.columnList = new ArrayList<>(arity);
        this.kind = kind;
    }

    public ColumnRowData(int arity) {
        this.columnList = new ArrayList<>(arity);
        // INSERT as default
        this.kind = RowKind.INSERT;
    }

    public void addHeader(String name) {
        if (this.header == null) {
            this.header = Maps.newHashMapWithExpectedSize(this.columnList.size());
        }
        this.header.put(name, this.header.size());
    }

    public void addExtHeader(String name) {
        this.extHeader.add(name);
    }

    public boolean isExtHeader(String name) {
        return extHeader.contains(name);
    }

    public Set<String> getExtHeader() {
        return extHeader;
    }

    public void addAllHeader(List<String> list) {
        for (String name : list) {
            this.addHeader(name);
        }
    }

    public Map<String, Integer> getHeaderInfo() {
        return header;
    }

    public String[] getHeaders() {
        if (this.header == null) {
            return null;
        }
        String[] names = new String[this.header.size()];
        for (Map.Entry<String, Integer> entry : header.entrySet()) {
            names[entry.getValue()] = entry.getKey();
        }
        return names;
    }

    public void addField(AbstractBaseColumn value) {
        this.columnList.add(value);
    }

    public void addAllField(List<AbstractBaseColumn> list) {
        this.columnList.addAll(list);
    }

    public void setField(int pos, AbstractBaseColumn value) {
        this.columnList.set(pos, value);
    }

    public AbstractBaseColumn getField(int pos) {
        return this.columnList.get(pos);
    }

    public AbstractBaseColumn getField(String name) {
        if (header == null) {
            return null;
        }
        Integer pos = header.getOrDefault(name, -1);
        return pos == -1 ? null : this.columnList.get(pos);
    }

    public ColumnRowData copy() {
        try {
            return InstantiationUtil.clone(this, Thread.currentThread().getContextClassLoader());
        } catch (Exception e) {
            throw new FlinkxRuntimeException(e);
        }
    }

    @Override
    public int getArity() {
        return columnList.size();
    }

    @Override
    public RowKind getRowKind() {
        return kind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        Preconditions.checkNotNull(kind);
        this.kind = kind;
    }

    @Override
    public boolean isNullAt(int pos) {
        return this.columnList.get(pos) == null || this.columnList.get(pos).getData() == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return this.columnList.get(pos).asBoolean();
    }

    @Override
    public byte getByte(int pos) {
        return this.columnList.get(pos).asBigDecimal().byteValue();
    }

    @Override
    public short getShort(int pos) {
        return this.columnList.get(pos).asShort();
    }

    @Override
    public int getInt(int pos) {
        return this.columnList.get(pos).asInt();
    }

    @Override
    public long getLong(int pos) {
        return this.columnList.get(pos).asLong();
    }

    @Override
    public float getFloat(int pos) {
        return this.columnList.get(pos).asFloat();
    }

    @Override
    public double getDouble(int pos) {
        return this.columnList.get(pos).asDouble();
    }

    @Override
    public StringData getString(int pos) {
        return StringData.fromString(this.columnList.get(pos).asString());
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        BigDecimal bigDecimal = this.columnList.get(pos).asBigDecimal();
        return DecimalData.fromBigDecimal(bigDecimal, bigDecimal.precision(), bigDecimal.scale());
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return TimestampData.fromTimestamp(this.columnList.get(pos).asTimestamp());
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return null;
    }

    @Override
    public byte[] getBinary(int pos) {
        return this.columnList.get(pos).asBinary();
    }

    @Override
    public ArrayData getArray(int pos) {
        return null;
    }

    @Override
    public MapData getMap(int pos) {
        return null;
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return null;
    }

    public String getString() {
        StringBuilder sb = new StringBuilder();
        return buildString(sb);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(kind.shortString());
        return buildString(sb);
    }

    private String buildString(StringBuilder sb) {
        sb.append("(");
        for (int i = 0; i < columnList.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(
                    StringUtils.arrayAwareToString(
                            (columnList.get(i) == null ? new NullColumn() : columnList.get(i))
                                    .asString()));
        }
        sb.append(")");
        return sb.toString();
    }
}
