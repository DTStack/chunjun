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

package com.dtstack.chunjun.util;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExternalDataUtil {

    public static void arrayDataToExternal(LogicalType type, Object[] to, ArrayData from) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                for (int i = 0; i < to.length; i++) {
                    to[i] = from.getBoolean(i);
                }
                break;
            case TINYINT:
                for (int i = 0; i < to.length; i++) {
                    to[i] = from.getByte(i);
                }
                break;
            case SMALLINT:
                for (int i = 0; i < to.length; i++) {
                    to[i] = from.getShort(i);
                }
                break;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                for (int i = 0; i < to.length; i++) {
                    to[i] = from.getInt(i);
                }
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                for (int i = 0; i < to.length; i++) {
                    to[i] = from.getLong(i);
                }
                break;
            case FLOAT:
                for (int i = 0; i < to.length; i++) {
                    to[i] = from.getFloat(i);
                }
                break;
            case DOUBLE:
                for (int i = 0; i < to.length; i++) {
                    to[i] = from.getDouble(i);
                }
                break;
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                for (int i = 0; i < to.length; i++) {
                    to[i] = from.getString(i) == null ? null : from.getString(i).toString();
                }
                break;
            case BINARY:
            case VARBINARY:
                for (int i = 0; i < to.length; i++) {
                    to[i] = from.getBinary(i);
                }
                break;
            case DATE:
                for (int i = 0; i < to.length; i++) {
                    to[i] = Date.valueOf(LocalDate.ofEpochDay(from.getInt(i)));
                }
                break;
            case TIME_WITHOUT_TIME_ZONE:
                for (int i = 0; i < to.length; i++) {
                    to[i] = Time.valueOf(LocalTime.ofNanoOfDay(from.getInt(i) * 1_000_000L));
                }
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                for (int i = 0; i < to.length; i++) {
                    to[i] = from.getTimestamp(i, timestampPrecision).toTimestamp();
                }
                break;
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                for (int i = 0; i < to.length; i++) {
                    to[i] = from.getDecimal(i, decimalPrecision, decimalScale).toBigDecimal();
                }
                break;
            case ROW:
                List<RowType.RowField> childrenFields = ((RowType) type).getFields();
                HashMap<String, Object> map = new HashMap<>();
                for (int i = 0; i < to.length; i++) {
                    RowData row = from.getRow(i, childrenFields.size());
                    HashMap<String, Object> map1 = new HashMap<>();
                    for (int j = 0; j < childrenFields.size(); j++) {
                        rowDataToExternal(
                                row,
                                j,
                                childrenFields.get(j).getType(),
                                map1,
                                childrenFields.get(j).getName());
                    }
                    map.put(childrenFields.get(i).getName(), map1);
                    to[i] = map;
                }
                break;
            case ARRAY:
                for (int i = 0; i < to.length; i++) {
                    LogicalType logicalType = type.getChildren().get(0);
                    ArrayData array = from.getArray(i);
                    Object[] objects = new Object[array.size()];
                    arrayDataToExternal(logicalType, objects, array);
                    to[i] = objects;
                }
                break;
            case MAP:
                for (int i = 0; i < to.length; i++) {
                    MapData tmpMap = from.getMap(i);
                    LogicalType keyType = ((MapType) type).getKeyType();
                    LogicalType valueType = ((MapType) type).getValueType();
                    Map<Object, Object> resultMap = new HashMap<>();
                    mapDataToExternal(tmpMap, keyType, valueType, resultMap);
                    to[i] = resultMap;
                }
                break;
            case MULTISET:
                for (int i = 0; i < to.length; i++) {
                    MapData tmpMap = from.getMap(i);
                    ArrayData arrayData = tmpMap.keyArray();
                    LogicalType logicalType = type.getChildren().get(0);
                    Object[] obj = new Object[arrayData.size()];
                    ExternalDataUtil.arrayDataToExternal(logicalType, obj, arrayData);
                    to[i] = obj;
                }
                break;
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public static void rowDataToExternal(
            RowData rowData, int pos, LogicalType type, Map<String, Object> map, String fieldName) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                map.put(fieldName, rowData.getBoolean(pos));
                break;
            case TINYINT:
                map.put(fieldName, rowData.getByte(pos));
                break;
            case SMALLINT:
                map.put(fieldName, rowData.getShort(pos));
                break;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                map.put(fieldName, rowData.getInt(pos));
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                map.put(fieldName, rowData.getLong(pos));
                break;
            case FLOAT:
                map.put(fieldName, rowData.getFloat(pos));
                break;
            case DOUBLE:
                map.put(fieldName, rowData.getDouble(pos));
                break;
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                String result =
                        rowData.getString(pos) == null ? null : rowData.getString(pos).toString();
                map.put(fieldName, result);
                break;
            case BINARY:
            case VARBINARY:
                map.put(fieldName, rowData.getBinary(pos));
                break;
            case DATE:
                map.put(fieldName, Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(pos))));
                break;
            case TIME_WITHOUT_TIME_ZONE:
                map.put(
                        fieldName,
                        Time.valueOf(LocalTime.ofNanoOfDay(rowData.getInt(pos) * 1_000_000L)));
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                map.put(fieldName, rowData.getTimestamp(pos, timestampPrecision).toTimestamp());
                break;
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                map.put(
                        fieldName,
                        rowData.getDecimal(pos, decimalPrecision, decimalScale).toBigDecimal());
                break;
            case ROW:
                List<RowType.RowField> childrenFields = ((RowType) type).getFields();
                RowData row = rowData.getRow(pos, childrenFields.size());
                HashMap<String, Object> map1 = new HashMap<>();
                for (int i = 0; i < childrenFields.size(); i++) {
                    rowDataToExternal(
                            row,
                            i,
                            childrenFields.get(i).getType(),
                            map1,
                            childrenFields.get(i).getName());
                }
                map.put(fieldName, map1);
                break;
            case ARRAY:
                ArrayData array = rowData.getArray(pos);
                Object[] objects = new Object[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    LogicalType logicalType = type.getChildren().get(0);
                    arrayDataToExternal(logicalType, objects, array);
                }
                map.put(fieldName, objects);
                break;
            case MAP:
                MapData tmpMap = rowData.getMap(pos);
                LogicalType keyType = ((MapType) type).getKeyType();
                LogicalType valueType = ((MapType) type).getValueType();
                Map<Object, Object> resultMap = new HashMap<>();
                mapDataToExternal(tmpMap, keyType, valueType, resultMap);
                map.put(fieldName, resultMap);
                break;
            case MULTISET:
                MapData map2 = rowData.getMap(pos);
                ArrayData arrayData = map2.keyArray();
                LogicalType logicalType = type.getChildren().get(0);
                Object[] obj = new Object[arrayData.size()];
                ExternalDataUtil.arrayDataToExternal(logicalType, obj, arrayData);
                map.put(fieldName, obj);
                break;
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public static void mapDataToExternal(
            MapData mapData, LogicalType keyType, LogicalType valueType, Map<Object, Object> map) {
        ArrayData keyArray = mapData.keyArray();
        ArrayData valueArray = mapData.valueArray();
        int size = keyArray.size();
        for (int i = 0; i < size; i++) {
            Object key = toExternal(keyArray, i, keyType);
            Object value = toExternal(valueArray, i, valueType);
            map.put(key, value);
        }
    }

    public static Object toExternal(ArrayData arrayData, int pos, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return arrayData.getBoolean(pos);
            case TINYINT:
                return arrayData.getByte(pos);
            case SMALLINT:
                return arrayData.getShort(pos);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return arrayData.getInt(pos);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return arrayData.getLong(pos);
            case FLOAT:
                return arrayData.getFloat(pos);
            case DOUBLE:
                return arrayData.getDouble(pos);
            case CHAR:
            case VARCHAR:
                // value is BinaryString

                return arrayData.getString(pos) == null
                        ? null
                        : arrayData.getString(pos).toString();
            case BINARY:
            case VARBINARY:
                return arrayData.getBinary(pos);
            case DATE:
                return Date.valueOf(LocalDate.ofEpochDay(arrayData.getInt(pos)));
            case TIME_WITHOUT_TIME_ZONE:
                return Time.valueOf(LocalTime.ofNanoOfDay(arrayData.getInt(pos) * 1_000_000L));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return arrayData.getTimestamp(pos, timestampPrecision).toTimestamp();
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return arrayData.getDecimal(pos, decimalPrecision, decimalScale).toBigDecimal();
            case ROW:
                List<RowType.RowField> childrenFields = ((RowType) type).getFields();
                RowData row = arrayData.getRow(pos, childrenFields.size());
                HashMap<String, Object> map1 = new HashMap<>();
                for (int i = 0; i < childrenFields.size(); i++) {
                    rowDataToExternal(
                            row,
                            i,
                            childrenFields.get(i).getType(),
                            map1,
                            childrenFields.get(i).getName());
                }
                return map1;
            case ARRAY:
                ArrayData array = arrayData.getArray(pos);
                Object[] objects = new Object[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    LogicalType logicalType = type.getChildren().get(0);
                    arrayDataToExternal(logicalType, objects, array);
                }
                return objects;
            case MAP:
                MapData map = arrayData.getMap(pos);
                LogicalType keyType = ((MapType) type).getKeyType();
                LogicalType valueType = ((MapType) type).getValueType();
                Map<Object, Object> resultMap = new HashMap<>();
                mapDataToExternal(map, keyType, valueType, resultMap);
                return resultMap;
            case MULTISET:
                MapData map2 = arrayData.getMap(pos);
                ArrayData keyArray = map2.keyArray();
                LogicalType logicalType = type.getChildren().get(0);
                Object[] obj = new Object[keyArray.size()];
                ExternalDataUtil.arrayDataToExternal(logicalType, obj, arrayData);
                return obj;
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
