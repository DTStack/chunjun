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
package com.dtstack.chunjun.element;

import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

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
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static com.dtstack.chunjun.element.ClassSizeUtil.getStringSize;

/**
 * Date: 2021/04/26 Company: www.dtstack.com
 *
 * @author tudou
 */
public final class ColumnRowData implements RowData, Serializable {

    private static final long serialVersionUID = 1L;
    private final List<AbstractBaseColumn> columnList;
    private LinkedHashMap<String, Integer> header;
    private Set<String> extHeader = new HashSet<>();
    private int byteSize;

    private RowKind kind;

    public ColumnRowData(RowKind kind, int arity) {
        this.columnList = new ArrayList<>(arity);
        this.kind = kind;
        // kind size
        byteSize = 1;
    }

    public ColumnRowData(RowKind kind, int arity, int byteSize) {
        this.columnList = new ArrayList<>(arity);
        this.kind = kind;
        this.byteSize = byteSize;
    }

    public ColumnRowData(int arity) {
        this(RowKind.INSERT, arity);
    }

    public void addHeader(String name) {
        if (this.header == null) {
            this.header = Maps.newLinkedHashMap();
        }
        this.header.put(name, this.header.size());
        byteSize += getStringSize(name);
    }

    public void setHeader(LinkedHashMap<String, Integer> header) {
        this.header = header;
    }

    public void setExtHeader(Set<String> extHeader) {
        this.extHeader = extHeader;
    }

    public void replaceHeader(String original, String another) {
        if (this.header == null || !this.header.containsKey(original)) {
            return;
        }
        Integer value = this.header.get(original);
        this.header.remove(original);
        this.header.put(another, value);
        byteSize -= getStringSize(original);
        byteSize += getStringSize(another);
    }

    public void addExtHeader(String name) {
        this.extHeader.add(name);
        byteSize += getStringSize(name);
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
            this.byteSize += getStringSize(name);
        }
    }

    public LinkedHashMap<String, Integer> getHeaderInfo() {
        return header;
    }

    public void removeExtHeaderInfo() {
        ArrayList<AbstractBaseColumn> newColumns = new ArrayList<>();
        LinkedHashMap<String, Integer> newHeader = new LinkedHashMap<>();

        if (CollectionUtils.isNotEmpty(extHeader)) {
            if (header.size() < columnList.size()) {
                ArrayList<Integer> indexs = new ArrayList<>();
                extHeader.forEach(
                        extHeaderName -> indexs.add(header.getOrDefault(extHeaderName, -1)));
                for (int i = 0; i < columnList.size(); i++) {
                    if (!indexs.contains(i)) {
                        newColumns.add(columnList.get(i));
                    }
                }
            } else {
                header.forEach(
                        (k, v) -> {
                            AbstractBaseColumn column = columnList.get(v);
                            if (extHeader.contains(k)) {
                                byteSize -= column.byteSize;
                            } else {
                                newHeader.put(k, newHeader.size());
                                newColumns.add(column);
                            }
                        });
            }

            this.columnList.clear();
            this.columnList.addAll(newColumns);
            this.header = newHeader;
        }
    }

    public String[] getHeaders() {
        if (this.header == null) {
            return null;
        }
        return header.keySet().toArray(new String[0]);
    }

    public void addField(AbstractBaseColumn value) {
        this.columnList.add(value);
        if (value != null) {
            this.byteSize += value.byteSize;
        }
    }

    public void addFieldWithOutByteSize(AbstractBaseColumn value) {
        this.columnList.add(value);
    }

    public void addAllField(List<AbstractBaseColumn> list) {
        for (AbstractBaseColumn column : list) {
            addField(column);
        }
    }

    public void setField(int pos, AbstractBaseColumn value) {
        if (columnList.get(pos) != null) {
            byteSize -= columnList.get(pos).byteSize;
        }
        this.columnList.set(pos, value);
        byteSize += value.byteSize;
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
            throw new ChunJunRuntimeException(e);
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

    public int getByteSize() {
        return byteSize;
    }

    public void setByteSize(int byteSize) {
        this.byteSize = byteSize;
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

    // sort rowData by field header name
    public ColumnRowData sortColumnRowData() {
        String[] oldHeaders = this.getHeaders();

        // mode: sync
        if (oldHeaders == null) {
            return this;
        }

        ColumnRowData newRowData = new ColumnRowData(this.getArity());
        String[] newHeaders = Arrays.stream(oldHeaders).sorted().toArray(String[]::new);

        newRowData.setRowKind(this.getRowKind());
        Arrays.stream(newHeaders)
                .forEach(
                        header -> {
                            AbstractBaseColumn column = this.getField(header);
                            newRowData.addHeader(header);
                            newRowData.addField(column);
                        });

        Set<String> extHeaders = this.getExtHeader();
        extHeaders.forEach(extHeader -> newRowData.addExtHeader(extHeader));
        return newRowData;
    }

    @Override
    public int hashCode() {
        if (columnList == null) {
            return 0;
        }
        int result = 1;
        for (AbstractBaseColumn column : columnList) {
            result = 31 * result + (column.data == null ? 0 : column.data.hashCode());
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ColumnRowData) {
            ColumnRowData that = (ColumnRowData) o;
            if (this.columnList.size() != that.columnList.size()) {
                return false;
            }
            Object thisData;
            Object thatData;
            for (int i = 0; i < this.columnList.size(); i++) {
                thisData = this.columnList.get(i).data;
                thatData = that.columnList.get(i).data;
                if (thisData == null) {
                    if (thatData != null) {
                        return false;
                    }
                } else if (!thisData.equals(thatData)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
