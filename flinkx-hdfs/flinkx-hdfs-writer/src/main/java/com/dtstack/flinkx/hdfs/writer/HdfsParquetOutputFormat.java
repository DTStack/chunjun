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

package com.dtstack.flinkx.hdfs.writer;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.hdfs.ECompressType;
import com.dtstack.flinkx.util.ColumnTypeUtil;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.lang.StringUtils;
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
public class HdfsParquetOutputFormat extends BaseHdfsOutputFormat {

    private SimpleGroupFactory groupFactory;

    private ParquetWriter<Group> writer;

    private MessageType schema;

    private static Calendar cal = Calendar.getInstance();

    private static final long NANO_SECONDS_PER_DAY = 86400_000_000_000L;

    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588;

    private static ColumnTypeUtil.DecimalInfo PARQUET_DEFAULT_DECIMAL_INFO = new ColumnTypeUtil.DecimalInfo(10, 0);

    static {
        try {
            cal.setTime(DateUtil.getDateFormatter().parse("1970-01-01"));
        } catch (Exception e){
            throw new RuntimeException("Init calendar fail:",e);
        }
    }

    @Override
    protected void openSource() throws IOException{
        super.openSource();

        schema = buildSchema();
        GroupWriteSupport.setSchema(schema,conf);
        groupFactory = new SimpleGroupFactory(schema);
    }

    @Override
    protected void nextBlock(){
        super.nextBlock();

        if (writer != null){
            return;
        }

        try {
            String currentBlockTmpPath = tmpPath + SP + currentBlockFileName;
            Path writePath = new Path(currentBlockTmpPath);
            ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(writePath)
                    .withWriteMode(ParquetFileWriter.Mode.CREATE)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                    .withCompressionCodec(getCompressType())
                    .withConf(conf)
                    .withType(schema)
                    .withRowGroupSize(rowGroupSize);
            writer = builder.build();

            blockIndex++;
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private CompressionCodecName getCompressType(){
        // Compatible with old code
        if(StringUtils.isEmpty(compress)){
            compress = ECompressType.PARQUET_SNAPPY.getType();
        }

        ECompressType compressType = ECompressType.getByTypeAndFileType(compress, "parquet");
        if(ECompressType.PARQUET_SNAPPY.equals(compressType)){
            return CompressionCodecName.SNAPPY;
        }else if(ECompressType.PARQUET_GZIP.equals(compressType)){
            return CompressionCodecName.GZIP;
        }else if(ECompressType.PARQUET_LZO.equals(compressType)){
            return CompressionCodecName.LZO;
        } else {
            return CompressionCodecName.UNCOMPRESSED;
        }
    }

    @Override
    protected String getExtension() {
        ECompressType compressType = ECompressType.getByTypeAndFileType(compress, "parquet");
        return compressType.getSuffix();
    }

    @Override
    public void flushDataInternal() throws IOException{
        LOG.info("Close current parquet record writer, write data size:[{}]", bytesWriteCounter.getLocalValue());

        if (writer != null){
            writer.close();
            writer = null;
        }
    }

    @Override
    public float getDeviation(){
        ECompressType compressType = ECompressType.getByTypeAndFileType(compress, "parquet");
        return compressType.getDeviation();
    }

    @Override
    public void writeSingleRecordToFile(Row row) throws WriteRecordException {

        if(writer == null){
            nextBlock();
        }

        Group group = groupFactory.newGroup();
        int i = 0;
        try {
            for (; i < fullColumnNames.size(); i++) {
                Object valObj = row.getField(colIndices[i]);
                if(valObj == null){
                    continue;
                }

                addDataToGroup(group, valObj, i);
            }
        } catch (Exception e){
            if(e instanceof WriteRecordException){
                throw (WriteRecordException) e;
            } else {
                throw new WriteRecordException(recordConvertDetailErrorMessage(i, row), e, i, row);
            }
        }

        try {
            writer.write(group);
            rowsOfCurrentBlock++;

            if(restoreConfig.isRestore()){
                lastRow = row;
            }
        } catch (IOException e) {
            throw new WriteRecordException(String.format("数据写入hdfs异常，row:{%s}", row), e);
        }
    }

    private void addDataToGroup(Group group, Object valObj, int i) throws Exception{
        String colName = fullColumnNames.get(i);
        String colType = fullColumnTypes.get(i);
        colType = ColumnType.fromString(colType).name().toLowerCase();

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
                ColumnTypeUtil.DecimalInfo decimalInfo = decimalColInfo.get(colName);

                HiveDecimal hiveDecimal = HiveDecimal.create(new BigDecimal(val));
                hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal, decimalInfo.getPrecision(), decimalInfo.getScale());
                if(hiveDecimal == null){
                    String msg = String.format("第[%s]个数据数据[%s]precision和scale和元数据不匹配:decimal(%s, %s)", i, decimalInfo.getPrecision(), decimalInfo.getScale(), valObj);
                    throw new WriteRecordException(msg, new IllegalArgumentException());
                }

                group.add(colName,decimalToBinary(hiveDecimal, decimalInfo.getPrecision(), decimalInfo.getScale()));
                break;
            case "date" :
                Date date = DateUtil.columnToDate(valObj,null);
                group.add(colName, DateWritable.dateToDays(new java.sql.Date(date.getTime())));
                break;
            default: group.add(colName,val);break;
        }
    }

    private Binary decimalToBinary(final HiveDecimal hiveDecimal, int prec,int scale) {
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

    @Override
    protected String recordConvertDetailErrorMessage(int pos, Row row) {
        return "\nHdfsParquetOutputFormat [" + jobName + "] writeRecord error: when converting field[" + fullColumnNames.get(pos) + "] in Row(" + row + ")";
    }

    @Override
    protected void closeSource() throws IOException {
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
                    if(ColumnTypeUtil.isDecimalType(colType)){
                        ColumnTypeUtil.DecimalInfo decimalInfo = ColumnTypeUtil.getDecimalInfo(colType, PARQUET_DEFAULT_DECIMAL_INFO);
                        typeBuilder.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                .as(OriginalType.DECIMAL)
                                .precision(decimalInfo.getPrecision())
                                .scale(decimalInfo.getScale())
                                .length(computeMinBytesForPrecision(decimalInfo.getPrecision()))
                                .named(name);

                        decimalColInfo = Collections.singletonMap(name, decimalInfo);
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
