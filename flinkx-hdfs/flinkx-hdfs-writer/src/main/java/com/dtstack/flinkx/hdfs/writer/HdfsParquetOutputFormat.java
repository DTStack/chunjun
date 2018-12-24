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

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
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
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * The subclass of HdfsOutputFormat writing parquet files
 *
 * Company: www.dtstack.com
 * @author jiangbo
 */
public class HdfsParquetOutputFormat extends HdfsOutputFormat {

    private SimpleGroupFactory groupFactory;

    private ParquetWriter<Group> writer;

    private static Calendar cal = Calendar.getInstance();

    private static final long NANO_SECONDS_PER_DAY = 86400_000_000_000L;

    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588;

    static {
        try {
            cal.setTime(DateUtil.getDateFormatter().parse("1970-01-01"));
        } catch (Exception e){
            e.printStackTrace();
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
                .withType(schema);
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
                String colType = fullColumnTypes.get(i).toLowerCase();

                Object valObj = row.getField(colIndices[i]);
                if(colType.matches(DateUtil.DATE_REGEX)){
                    valObj = DateUtil.columnToDate(valObj,null);
                } else if(colType.matches(DateUtil.DATETIME_REGEX) || colType.matches(DateUtil.TIMESTAMP_REGEX)){
                    valObj = DateUtil.columnToTimestamp(valObj,null);
                }

                String val = null;
                if(valObj != null){
                    if(valObj instanceof java.sql.Timestamp){
                        val = String.valueOf(((java.sql.Timestamp) valObj).getTime());
                    } else if(valObj instanceof Date){
                        val = DateUtil.getDateFormatter().format((Date)valObj);
                    } else {
                        val = String.valueOf(valObj);
                    }
                }

                if (val == null){
                    continue;
                }

                switch (colType){
                    case "tinyint" :
                    case "smallint" :
                    case "int" : group.add(colName,Integer.parseInt(val));break;
                    case "bigint" : group.add(colName,Long.parseLong(val));break;
                    case "float" : group.add(colName,Float.parseFloat(val));break;
                    case "double" : group.add(colName,Double.parseDouble(val));break;
                    case "binary" :group.add(colName,Binary.fromString(val));break;
                    case "char" :
                    case "varchar" :
                    case "string" : group.add(colName,val);break;
                    case "boolean" : group.add(colName,Boolean.parseBoolean(val));break;
                    case "timestamp" :
                        byte[] dst = timeToByteArray(val);
                        group.add(colName, Binary.fromConstantByteArray(dst));
                        break;
                    case "decimal" : group.add(colName,val);break;
                    case "date" : group.add(colName,getDay(val));break;
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
                        typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(OriginalType.DECIMAL)
                                .precision(precision)
                                .scale(scale)
                                .named(name);
                    } else {
                        typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
                    }
                    break;
            }
        }

        return typeBuilder.named("Pair");
    }

    private static byte[] timeToByteArray(String val) throws ParseException {
        byte[] dst;
        if(NumberUtils.isNumber(val)){
            dst = longToByteArray(Long.parseLong(val));
        } else {
            Date date = DateUtil.getDateTimeFormatter().parse(val);
            dst = longToByteArray(date.getTime());
        }

        return dst;
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

    private int getDay(String dateStr) throws Exception{
        Date date = DateUtil.getDateFormatter().parse(dateStr);
        return (int)((date.getTime() - cal.getTimeInMillis()) / (1000 * 60 * 60 * 24));
    }
}
