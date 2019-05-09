/**
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

package com.dtstack.flinkx.hdfs.writer;

import com.dtstack.flinkx.common.ColumnType;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;

/**
 * The subclass of HdfsOutputFormat writing parquet files
 *
 * Company: www.dtstack.com
 * @author jiangbo
 */
public class HdfsParquetOutputFormat extends HdfsOutputFormat {

    private SimpleGroupFactory groupFactory;

    private ParquetWriter<Group> writer;

    private Map<String, Map<String,Integer>> decimalColInfo;

    private static final String KEY_PRECISION = "precision";

    private static final String KEY_SCALE = "scale";

    private static final int DEFAULT_PRECISION = 10;

    private static final int DEFAULT_SCALE = 0;

    private static Calendar cal = Calendar.getInstance();

    private static final long NANO_SECONDS_PER_DAY = 86400_000_000_000L;

    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588;

    static {
        try {
            cal.setTime(DateUtil.getDateFormatter().parse("1970-01-01"));
        } catch (Exception e){
            throw new RuntimeException("Init calendar fail:",e);
        }
    }

    @Override
    protected void open() throws IOException {
        MessageType schema = buildSchema();
        GroupWriteSupport.setSchema(schema,conf);
        Path writePath = new Path(tmpPath);

        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(writePath)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(conf)
                .withType(schema)
                .withRowGroupSize(rowGroupSize);
        writer = builder.build();
        groupFactory = new SimpleGroupFactory(schema);
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        Group group = groupFactory.newGroup();
        int i = 0;
        try {
            for (; i < fullColumnNames.size(); i++) {
                String colName = fullColumnNames.get(i);
                String colType = fullColumnTypes.get(i);
                colType = ColumnType.fromString(colType).name().toLowerCase();

                Object valObj = row.getField(colIndices[i]);

                if(valObj == null){
                    continue;
                }

                String val = valObj.toString();

                switch (colType){
                    case "tinyint" :
                    case "smallint" :
                    case "int" :
                        if (valObj instanceof Timestamp){
                            ((Timestamp) valObj).getTime();
                            group.add(colName,(int)((Timestamp) valObj).getTime());
                        } else if(valObj instanceof Date){
                            group.add(colName,(int)((Date) valObj).getTime());
                        } else {
                            group.add(colName,Integer.parseInt(val));
                        }
                        break;
                    case "bigint" :
                        if (valObj instanceof Timestamp){
                            group.add(colName,((Timestamp) valObj).getTime());
                        } else if(valObj instanceof Date){
                            group.add(colName,((Date) valObj).getTime());
                        } else {
                            group.add(colName,Long.parseLong(val));
                        }
                        break;
                    case "float" : group.add(colName,Float.parseFloat(val));break;
                    case "double" : group.add(colName,Double.parseDouble(val));break;
                    case "binary" :group.add(colName,Binary.fromString(val));break;
                    case "char" :
                    case "varchar" :
                    case "string" :
                        if (valObj instanceof Timestamp){
                            val=DateUtil.getDateTimeFormatter().format(valObj);
                            group.add(colName,val);
                        }else {
                            group.add(colName,val);
                        }
                        break;
                    case "boolean" : group.add(colName,Boolean.parseBoolean(val));break;
                    case "timestamp" :
                        Timestamp ts = DateUtil.columnToTimestamp(valObj,null);
                        byte[] dst = longToByteArray(ts.getTime());
                        group.add(colName, Binary.fromConstantByteArray(dst));
                        break;
                    case "decimal" :
                        HiveDecimal hiveDecimal = HiveDecimal.create(new BigDecimal(val));
                        Map<String,Integer> decimalInfo = decimalColInfo.get(colName);
                        if(decimalInfo != null){
                            group.add(colName,decimalToBinary(hiveDecimal,decimalInfo.get(KEY_PRECISION),decimalInfo.get(KEY_SCALE)));
                        } else {
                            group.add(colName,decimalToBinary(hiveDecimal,DEFAULT_PRECISION,DEFAULT_SCALE));
                        }
                        break;
                    case "date" :
                        Date date = DateUtil.columnToDate(valObj,null);
                        group.add(colName, DateWritable.dateToDays(new java.sql.Date(date.getTime())));
                        break;
                    default: group.add(colName,val);break;
                }
            }

            writer.write(group);
        } catch (Exception e){
            if(i < row.getArity()) {
                throw new WriteRecordException(recordConvertDetailErrorMessage(i, row), e, i, row);
            }
            throw new WriteRecordException(e.getMessage(), e);
        }
    }

    private Binary decimalToBinary(final HiveDecimal hiveDecimal, int prec,int scale) {
        byte[] decimalBytes = hiveDecimal.setScale(scale).unscaledValue().toByteArray();

        // Estimated number of bytes needed.
        int precToBytes = ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[prec - 1];
        if (precToBytes == decimalBytes.length) {
            // No padding needed.
            return Binary.fromByteArray(decimalBytes);
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
        return Binary.fromByteArray(tgt);
    }

    @Override
    protected String recordConvertDetailErrorMessage(int pos, Row row) {
        return "\nHdfsParquetOutputFormat [" + jobName + "] writeRecord error: when converting field[" + fullColumnNames.get(pos) + "] in Row(" + row + ")";
    }

    @Override
    public void closeInternal() throws IOException {
        if (writer != null){
            writer.close();
        }
    }

    private MessageType buildSchema(){
        decimalColInfo = new HashMap<>();
        Types.MessageTypeBuilder typeBuilder = Types.buildMessage();
        for (int i = 0; i < fullColumnNames.size(); i++) {
            String name = fullColumnNames.get(i);
            String colType = fullColumnTypes.get(i).toLowerCase();
            switch (colType){
                case "tinyint" :
                case "smallint" :
                case "int" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(name);break;
                case "bigint" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name);break;
                case "float" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(name);break;
                case "double" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);break;
                case "binary" :typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);break;
                case "char" :
                case "varchar" :
                case "string" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(name);break;
                case "boolean" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);break;
                case "timestamp" : typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT96).named(name);break;
                case "date" :typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT32).as(OriginalType.DATE).named(name);break;
                default:
                    if (colType.contains("decimal")){
                        int precision = Integer.parseInt(colType.substring(colType.indexOf("(") + 1,colType.indexOf(",")).trim());
                        int scale = Integer.parseInt(colType.substring(colType.indexOf(",") + 1,colType.indexOf(")")).trim());
                        typeBuilder.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                .as(OriginalType.DECIMAL)
                                .precision(precision)
                                .scale(scale)
                                .length(computeMinBytesForPrecision(precision))
                                .named(name);

                        Map<String,Integer> decimalInfo = new HashMap<>();
                        decimalInfo.put(KEY_PRECISION,precision);
                        decimalInfo.put(KEY_SCALE,scale);
                        decimalColInfo.put(name,decimalInfo);
                    } else {
                        typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
                    }
                    break;
            }
        }

        return typeBuilder.named("Pair");
    }

    private int computeMinBytesForPrecision(int precision){
        int numBytes = 1;
        while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    private static byte[] longToByteArray(long data){
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
