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

package com.dtstack.chunjun.connector.hbase14.converter;

import com.dtstack.chunjun.enums.ColumnType;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.Charset;
import java.sql.Timestamp;

/**
 * @program chunjun
 * @author: wuren
 * @create: 2021/10/19
 */
public class DataSyncConverterUtils {

    public static byte[] getValueByte(ColumnType columnType, String value, String encoding) {
        byte[] bytes;
        if (value != null) {
            switch (columnType) {
                case INT:
                    bytes = Bytes.toBytes(Integer.parseInt(value));
                    break;
                case LONG:
                    bytes = Bytes.toBytes(Long.parseLong(value));
                    break;
                case DOUBLE:
                    bytes = Bytes.toBytes(Double.parseDouble(value));
                    break;
                case FLOAT:
                    bytes = Bytes.toBytes(Float.parseFloat(value));
                    break;
                case SHORT:
                    bytes = Bytes.toBytes(Short.parseShort(value));
                    break;
                case BOOLEAN:
                    bytes = Bytes.toBytes(Boolean.parseBoolean(value));
                    break;
                case STRING:
                    bytes = value.getBytes(Charset.forName(encoding));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column type: " + columnType);
            }
        } else {
            bytes = HConstants.EMPTY_BYTE_ARRAY;
        }
        return bytes;
    }

    public static byte[] intToBytes(Object column) {
        Integer intValue = null;
        if (column instanceof Integer) {
            intValue = (Integer) column;
        } else if (column instanceof Long) {
            intValue = ((Long) column).intValue();
        } else if (column instanceof Double) {
            intValue = ((Double) column).intValue();
        } else if (column instanceof Float) {
            intValue = ((Float) column).intValue();
        } else if (column instanceof Short) {
            intValue = ((Short) column).intValue();
        } else if (column instanceof Boolean) {
            intValue = (Boolean) column ? 1 : 0;
        } else if (column instanceof String) {
            intValue = Integer.valueOf((String) column);
        } else {
            throw new RuntimeException("Can't convert from " + column.getClass() + " to INT");
        }

        return Bytes.toBytes(intValue);
    }

    public static byte[] longToBytes(Object column) {
        Long longValue = null;
        if (column instanceof Integer) {
            longValue = ((Integer) column).longValue();
        } else if (column instanceof Long) {
            longValue = (Long) column;
        } else if (column instanceof Double) {
            longValue = ((Double) column).longValue();
        } else if (column instanceof Float) {
            longValue = ((Float) column).longValue();
        } else if (column instanceof Short) {
            longValue = ((Short) column).longValue();
        } else if (column instanceof Boolean) {
            longValue = (Boolean) column ? 1L : 0L;
        } else if (column instanceof String) {
            longValue = Long.valueOf((String) column);
        } else if (column instanceof Timestamp) {
            longValue = ((Timestamp) column).getTime();
        } else {
            throw new RuntimeException("Can't convert from " + column.getClass() + " to LONG");
        }

        return Bytes.toBytes(longValue);
    }

    public static byte[] doubleToBytes(Object column) {
        Double doubleValue;
        if (column instanceof Integer) {
            doubleValue = ((Integer) column).doubleValue();
        } else if (column instanceof Long) {
            doubleValue = ((Long) column).doubleValue();
        } else if (column instanceof Double) {
            doubleValue = (Double) column;
        } else if (column instanceof Float) {
            doubleValue = ((Float) column).doubleValue();
        } else if (column instanceof Short) {
            doubleValue = ((Short) column).doubleValue();
        } else if (column instanceof Boolean) {
            doubleValue = (Boolean) column ? 1.0 : 0.0;
        } else if (column instanceof String) {
            doubleValue = Double.valueOf((String) column);
        } else {
            throw new RuntimeException("Can't convert from " + column.getClass() + " to DOUBLE");
        }

        return Bytes.toBytes(doubleValue);
    }

    public static byte[] floatToBytes(Object column) {
        Float floatValue = null;
        if (column instanceof Integer) {
            floatValue = ((Integer) column).floatValue();
        } else if (column instanceof Long) {
            floatValue = ((Long) column).floatValue();
        } else if (column instanceof Double) {
            floatValue = ((Double) column).floatValue();
        } else if (column instanceof Float) {
            floatValue = (Float) column;
        } else if (column instanceof Short) {
            floatValue = ((Short) column).floatValue();
        } else if (column instanceof Boolean) {
            floatValue = (Boolean) column ? 1.0f : 0.0f;
        } else if (column instanceof String) {
            floatValue = Float.valueOf((String) column);
        } else {
            throw new RuntimeException("Can't convert from " + column.getClass() + " to DOUBLE");
        }

        return Bytes.toBytes(floatValue);
    }

    public static byte[] shortToBytes(Object column) {
        Short shortValue = null;
        if (column instanceof Integer) {
            shortValue = ((Integer) column).shortValue();
        } else if (column instanceof Long) {
            shortValue = ((Long) column).shortValue();
        } else if (column instanceof Double) {
            shortValue = ((Double) column).shortValue();
        } else if (column instanceof Float) {
            shortValue = ((Float) column).shortValue();
        } else if (column instanceof Short) {
            shortValue = (Short) column;
        } else if (column instanceof Boolean) {
            shortValue = (Boolean) column ? (short) 1 : (short) 0;
        } else if (column instanceof String) {
            shortValue = Short.valueOf((String) column);
        } else {
            throw new RuntimeException("Can't convert from " + column.getClass() + " to SHORT");
        }
        return Bytes.toBytes(shortValue);
    }

    public static byte[] boolToBytes(Object column) {
        Boolean booleanValue = null;
        if (column instanceof Integer) {
            booleanValue = (Integer) column != 0;
        } else if (column instanceof Long) {
            booleanValue = (Long) column != 0L;
        } else if (column instanceof Double) {
            booleanValue = new Double(0.0).compareTo((Double) column) != 0;
        } else if (column instanceof Float) {
            booleanValue = new Float(0.0f).compareTo((Float) column) != 0;
        } else if (column instanceof Short) {
            booleanValue = (Short) column != 0;
        } else if (column instanceof Boolean) {
            booleanValue = (Boolean) column;
        } else if (column instanceof String) {
            booleanValue = Boolean.valueOf((String) column);
        } else {
            throw new RuntimeException("Can't convert from " + column.getClass() + " to SHORT");
        }

        return Bytes.toBytes(booleanValue);
    }
}
