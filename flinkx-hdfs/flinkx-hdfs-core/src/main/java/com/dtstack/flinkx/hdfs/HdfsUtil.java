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

package com.dtstack.flinkx.hdfs;

import com.dtstack.flinkx.enums.ColumnType;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.parquet.io.api.Binary;


/**
 * Utilities for HdfsReader and HdfsWriter
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsUtil {

    public static final String NULL_VALUE = "\\N";

    private static final long NANO_SECONDS_PER_DAY = 86400_000_000_000L;

    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588;

    private static final double SCALE_TWO = 2.0;
    private static final double SCALE_TEN = 10.0;
    private static final int BIT_SIZE = 8;

    public static Object getWritableValue(Object writable) {
        Class<?> clz = writable.getClass();
        Object ret = null;

        if(clz == IntWritable.class) {
            ret = ((IntWritable) writable).get();
        } else if(clz == Text.class) {
            ret = ((Text) writable).toString();
        } else if(clz == LongWritable.class) {
            ret = ((LongWritable) writable).get();
        } else if(clz == ByteWritable.class) {
            ret = ((ByteWritable) writable).get();
        } else if(clz == DateWritable.class) {
            ret = ((DateWritable) writable).get();
        } else if(writable instanceof DoubleWritable){
            ret = ((DoubleWritable) writable).get();
        } else if(writable instanceof TimestampWritable){
            ret = ((TimestampWritable) writable).getTimestamp();
        }else if (writable instanceof DateWritable){
            ret = ((DateWritable) writable).get();
        }else if (writable instanceof FloatWritable){
            ret = ((FloatWritable) writable).get();
        }else if (writable instanceof BooleanWritable){
            ret = ((BooleanWritable) writable).get();
        }else  {
            ret = writable.toString();
        }
        return ret;
    }

    public static ObjectInspector columnTypeToObjectInspetor(ColumnType columnType) {
        ObjectInspector objectInspector = null;
        switch(columnType) {
            case TINYINT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case SMALLINT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case INT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BIGINT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case FLOAT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DOUBLE:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DECIMAL:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(HiveDecimalWritable.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case TIMESTAMP:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Timestamp.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DATE:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BOOLEAN:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BINARY:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(BytesWritable.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            default:
                throw new IllegalArgumentException("You should not be here");
        }
        return objectInspector;
    }


    public static Binary decimalToBinary(final HiveDecimal hiveDecimal, int prec, int scale) {
        byte[] decimalBytes = hiveDecimal.setScale(scale).unscaledValue().toByteArray();

        // Estimated number of bytes needed.
        int precToBytes = ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[prec - 1];
        if (precToBytes == decimalBytes.length) {
            // No padding needed.
            return Binary.fromReusedByteArray(decimalBytes);
        }

        byte[] tgt = new byte[precToBytes];
        if (hiveDecimal.signum() == -1) {
            // For negative number, initializing bits to 1
            for (int i = 0; i < precToBytes; i++) {
                tgt[i] |= 0xFF;
            }
        }

        // Padding leading zeroes/ones.
        System.arraycopy(decimalBytes, 0, tgt, precToBytes - decimalBytes.length, decimalBytes.length);
        return Binary.fromReusedByteArray(tgt);
    }

    public static int computeMinBytesForPrecision(int precision){
        int numBytes = 1;
        while (Math.pow(SCALE_TWO, BIT_SIZE * numBytes - 1.0) < Math.pow(SCALE_TEN, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    public static byte[] longToByteArray(long data){
        long nano = data * 1000_000;

        int julianDays = (int) ((nano / NANO_SECONDS_PER_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
        byte[] julianDaysBytes = getBytes(julianDays);
        flip(julianDaysBytes);

        long lastDayNanos = nano % NANO_SECONDS_PER_DAY;
        byte[] lastDayNanosBytes = getBytes(lastDayNanos);
        flip(lastDayNanosBytes);

        byte[] dst = new byte[12];

        System.arraycopy(lastDayNanosBytes, 0, dst, 0, 8);
        System.arraycopy(julianDaysBytes, 0, dst, 8, 4);

        return dst;
    }

    private static byte[] getBytes(long i) {
        byte[] bytes=new byte[8];
        bytes[0]=(byte)((i >> 56) & 0xFF);
        bytes[1]=(byte)((i >> 48) & 0xFF);
        bytes[2]=(byte)((i >> 40) & 0xFF);
        bytes[3]=(byte)((i >> 32) & 0xFF);
        bytes[4]=(byte)((i >> 24) & 0xFF);
        bytes[5]=(byte)((i >> 16) & 0xFF);
        bytes[6]=(byte)((i >> 8) & 0xFF);
        bytes[7]=(byte)(i & 0xFF);
        return bytes;
    }

    /**
     * @param bytes
     */
    private static void flip(byte[] bytes) {
        for(int i=0,j=bytes.length-1;i<j;i++,j--) {
            byte t=bytes[i];
            bytes[i]=bytes[j];
            bytes[j]=t;
        }
    }
}
