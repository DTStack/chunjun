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

import com.dtstack.flinkx.util.JsonUtil;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Date: 2021/04/26
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public final class ColumnRowData implements RowData, Serializable {

    private static final long serialVersionUID = 1L;
    private final List<AbstractBaseColumn> fields;
    private Map<String, Integer> header;

    private RowKind kind;

    public ColumnRowData(RowKind kind, int arity) {
        this.fields = new ArrayList<>(arity);
        this.kind = kind;
    }

    public ColumnRowData(int arity) {
        this.fields = new ArrayList<>(arity);
        this.kind = RowKind.INSERT; // INSERT as default
    }

    public void addHeader(String name){
        if(this.header == null){
            this.header = Maps.newHashMapWithExpectedSize(this.fields.size());
        }
        this.header.put(name, this.header.size());
    }

    public void addAllHeader(List<String> list){
        for (String name : list) {
            this.addHeader(name);
        }
    }

    public String[] getHeaders(){
        if(this.header == null){
            return null;
        }
        String[] names = new String[this.header.size()];
        for (Map.Entry<String, Integer> entry : header.entrySet()) {
            names[entry.getValue()] = entry.getKey();
        }
        return names;
    }

    public void addField(AbstractBaseColumn value) {
        this.fields.add(value);
    }

    public void addAllField(List<AbstractBaseColumn> list) {
        this.fields.addAll(list);
    }

    public void setField(int pos, AbstractBaseColumn value) {
        this.fields.set(pos, value);
    }

    public AbstractBaseColumn getField(int pos) {
        return this.fields.get(pos);
    }

    public AbstractBaseColumn getField(String name) {
        if(header == null){
            return null;
        }
        Integer pos = header.getOrDefault(name, -1);
        return pos == -1 ? null : this.fields.get(pos);
    }

    public ColumnRowData copy(){
        try {
            return InstantiationUtil.clone(this, Thread.currentThread().getContextClassLoader());
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getArity() {
        return fields.size();
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
        return this.fields.get(pos) == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return this.fields.get(pos).asBoolean();
    }

    @Override
    public byte getByte(int pos) {
        return this.fields.get(pos).asBigDecimal().byteValue();
    }

    @Override
    public short getShort(int pos) {
        return this.fields.get(pos).asShort();
    }

    @Override
    public int getInt(int pos) {
        return this.fields.get(pos).asInt();
    }

    @Override
    public long getLong(int pos) {
        return this.fields.get(pos).asLong();
    }

    @Override
    public float getFloat(int pos) {
        return this.fields.get(pos).asFloat();
    }

    @Override
    public double getDouble(int pos) {
        return this.fields.get(pos).asDouble();
    }

    @Override
    public StringData getString(int pos) {
        return StringData.fromString(this.fields.get(pos).asString());
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        BigDecimal bigDecimal = this.fields.get(pos).asBigDecimal();
        return DecimalData.fromBigDecimal(bigDecimal, bigDecimal.precision(), bigDecimal.scale());
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return TimestampData.fromTimestamp(this.fields.get(pos).asTimestamp());
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return null;
    }

    @Override
    public byte[] getBinary(int pos) {
        return this.fields.get(pos).asBinary();
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

    @Override
    public String toString() {
        return JsonUtil.toPrintJson(this);
    }
}
