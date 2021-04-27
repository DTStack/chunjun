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

import com.dtstack.flinkx.enums.OperationType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Date: 2021/04/26
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public final class ColumnRowData implements RowData {

    private final List<AbstractBaseColumn> fields;

    private RowKind kind;
    private String database;
    private String schema;
    private String table;
    private OperationType operationType;

    public ColumnRowData(RowKind kind, int arity) {
        this.fields = new ArrayList<>(arity);
        this.kind = kind;
    }

    public ColumnRowData(int arity) {
        this.fields = new ArrayList<>(arity);
        this.kind = RowKind.INSERT; // INSERT as default
    }

    public void addField(AbstractBaseColumn value) {
        this.fields.add(value);
    }

    public void setField(int pos, AbstractBaseColumn value) {
        this.fields.set(pos, value);
    }

    public AbstractBaseColumn getField(int pos) {
        return this.fields.get(pos);
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
        checkNotNull(kind);
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
        // todo
        return this.fields.get(pos).asBigDecimal().byteValue();
    }

    @Override
    public short getShort(int pos) {
        return this.fields.get(pos).asBigDecimal().shortValue();
    }

    @Override
    public int getInt(int pos) {
        return this.fields.get(pos).asBigDecimal().intValue();
    }

    @Override
    public long getLong(int pos) {
        return this.fields.get(pos).asBigDecimal().longValue();
    }

    @Override
    public float getFloat(int pos) {
        return this.fields.get(pos).asBigDecimal().floatValue();
    }

    @Override
    public double getDouble(int pos) {
        return this.fields.get(pos).asBigDecimal().doubleValue();
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
        return new byte[0];
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

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
