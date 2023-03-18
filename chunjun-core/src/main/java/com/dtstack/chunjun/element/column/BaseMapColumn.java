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

package com.dtstack.chunjun.element.column;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.throwable.CastException;

import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.fastjson.JSON;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class BaseMapColumn extends AbstractBaseColumn {

    public BaseMapColumn(Map<?, ?> data) {
        super(data, data.toString().length());
    }

    public BaseMapColumn(Map<?, ?> data, LogicalType keyType, LogicalType valueType) {
        super(data, 0);
        this.data = asBaseMap(keyType, valueType);
        this.byteSize = data.toString().length();
    }

    private BaseMapColumn(Map<?, ?> data, int byteSize) {
        super(data, byteSize);
    }

    public static BaseMapColumn from(Map<?, ?> data) {
        return new BaseMapColumn(data, 0);
    }

    @Override
    public String type() {
        return "MAP";
    }

    @Override
    public Boolean asBooleanInternal() {
        throw new CastException("MAP", "BOOLEAN", this.asStringInternal());
    }

    @Override
    public byte[] asBytesInternal() {
        throw new CastException("MAP", "BYTES", this.asStringInternal());
    }

    @Override
    public String asStringInternal() {
        return JSON.toJSONString(data);
    }

    @Override
    public BigDecimal asBigDecimalInternal() {
        throw new CastException("MAP", "BigDecimal", this.asStringInternal());
    }

    @Override
    public Timestamp asTimestampInternal() {
        throw new CastException("MAP", "java.sql.Timestamp", this.asStringInternal());
    }

    @Override
    public Time asTimeInternal() {
        throw new CastException("MAP", "java.sql.Time", this.asStringInternal());
    }

    @Override
    public Date asSqlDateInternal() {
        throw new CastException("MAP", "java.sql.Date", this.asStringInternal());
    }

    @Override
    public String asTimestampStrInternal() {
        throw new CastException("MAP", "java.sql.Timestamp", this.asStringInternal());
    }

    @Override
    public Map<?, ?> asBaseMap() {
        return (Map<?, ?>) this.data;
    }

    @Override
    public Map<?, ?> asBaseMap(LogicalType keyType, LogicalType valueType) {
        Object[] keyColumn = (Object[]) keySet(keyType, true).asArray();
        Object[] valueColumn = (Object[]) valueSet(valueType, true).asArray();
        Map<Object, Object> map = new HashMap<>(keyColumn.length);
        for (int i = 0; i < keyColumn.length; i++) {
            map.put(keyColumn[i], valueColumn[i]);
        }
        return map;
    }

    public ArrayColumn keySet(LogicalType keyType) {
        return keySet(keyType, false);
    }

    private ArrayColumn keySet(LogicalType keyType, boolean check) {
        return toArrayColumnInternal(((Map<?, ?>) data).keySet().toArray(), keyType, check);
    }

    public ArrayColumn valueSet(LogicalType valueType) {
        return valueSet(valueType, false);
    }

    private ArrayColumn valueSet(LogicalType valueType, boolean check) {
        return toArrayColumnInternal(((Map<?, ?>) data).values().toArray(), valueType, check);
    }

    private ArrayColumn toArrayColumnInternal(Object[] objects, LogicalType type, boolean check) {
        if (check) {
            return new ArrayColumn(objects, type);
        } else {
            return ArrayColumn.from(objects, objects.length, false);
        }
    }

    public Object get(Object key) {
        return ((Map<?, ?>) data).get(key);
    }
}
