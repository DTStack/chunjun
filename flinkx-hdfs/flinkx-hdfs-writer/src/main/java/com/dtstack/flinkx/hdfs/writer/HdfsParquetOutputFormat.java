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
import com.dtstack.flinkx.hdfs.HdfsUtil;
import com.dtstack.flinkx.util.ColumnTypeUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.FileSystemUtil;
import com.dtstack.flinkx.util.GsonUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.math.BigDecimal;
import java.security.PrivilegedAction;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private static ColumnTypeUtil.DecimalInfo PARQUET_DEFAULT_DECIMAL_INFO = new ColumnTypeUtil.DecimalInfo(10, 0);

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
                    .withDictionaryEncoding(enableDictionary)
                    .withRowGroupSize(rowGroupSize);

            //开启kerberos 需要在ugi里进行build
            if(FileSystemUtil.isOpenKerberos(hadoopConfig)){
                UserGroupInformation ugi = FileSystemUtil.getUGI(hadoopConfig, defaultFs);
                ugi.doAs((PrivilegedAction<Object>) () -> {
                    try {
                        writer = builder.build();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });
            }else{
                writer = builder.build();
            }
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
                int colIndex = colIndices[i];
                if(colIndex > -1){
                    Object valObj = row.getField(colIndex);
                    if(valObj == null || (valObj.toString().length() == 0 && !ColumnType.isStringType(fullColumnTypes.get(i)))){
                        continue;
                    }

                    addDataToGroup(group, valObj, i);
                }
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
                    val=DateUtil.getDateTimeFormatterForMillisencond().format(valObj);
                    group.add(colName,val);
                }else if (valObj instanceof Map || valObj instanceof List){
                    group.add(colName,gson.toJson(valObj));
                }else {
                    group.add(colName,val);
                }
                break;
            case "boolean" : group.add(colName,Boolean.parseBoolean(val));break;
            case "timestamp" :
                Timestamp ts = DateUtil.columnToTimestamp(valObj,null);
                byte[] dst = HdfsUtil.longToByteArray(ts.getTime());
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

                group.add(colName, HdfsUtil.decimalToBinary(hiveDecimal, decimalInfo.getPrecision(), decimalInfo.getScale()));
                break;
            case "date" :
                Date date = DateUtil.columnToDate(valObj,null);
                group.add(colName, DateWritable.dateToDays(new java.sql.Date(date.getTime())));
                break;
            default: group.add(colName,val);break;
        }
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
        decimalColInfo = new HashMap<>(16);
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
                                .length(HdfsUtil.computeMinBytesForPrecision(decimalInfo.getPrecision()))
                                .named(name);

                        decimalColInfo.put(name, decimalInfo);
                    } else {
                        typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
                    }
                    break;
            }
        }

        return typeBuilder.named("Pair");
    }
}
