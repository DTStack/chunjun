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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

/**
 * Date: 2021/04/28 Company: www.dtstack.com
 *
 * @author tudou
 */
public class ErrorMsgRowData implements RowData {

    private final String errorMsg;

    public ErrorMsgRowData(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public RowKind getRowKind() {
        return RowKind.INSERT;
    }

    @Override
    public void setRowKind(RowKind kind) {}

    @Override
    public boolean isNullAt(int pos) {
        return false;
    }

    @Override
    public boolean getBoolean(int pos) {
        return false;
    }

    @Override
    public byte getByte(int pos) {
        return 0;
    }

    @Override
    public short getShort(int pos) {
        return 0;
    }

    @Override
    public int getInt(int pos) {
        return 0;
    }

    @Override
    public long getLong(int pos) {
        return 0;
    }

    @Override
    public float getFloat(int pos) {
        return 0;
    }

    @Override
    public double getDouble(int pos) {
        return 0;
    }

    @Override
    public StringData getString(int pos) {
        return null;
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return null;
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return null;
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

    @Override
    public String toString() {
        return errorMsg;
    }
}
