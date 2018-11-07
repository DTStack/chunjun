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

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    private static Calendar cal = Calendar.getInstance();

    static {
        try {
            cal.setTime(sdf.parse("1970-01-01"));
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
                String val = "";
                if(valObj != null){
                    if(valObj instanceof Date){
                        val = sdf.format((Date)valObj);
                    } else {
                        val = String.valueOf(valObj);
                    }
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
                    case "timestamp" : group.add(colName,Long.parseLong(val));break;
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
                case "int" : typeBuilder.required(PrimitiveType.PrimitiveTypeName.INT32).named(name);break;
                case "bigint" : typeBuilder.required(PrimitiveType.PrimitiveTypeName.INT64).named(name);break;
                case "float" : typeBuilder.required(PrimitiveType.PrimitiveTypeName.FLOAT).named(name);break;
                case "double" : typeBuilder.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);break;
                case "binary" :typeBuilder.required(PrimitiveType.PrimitiveTypeName.BINARY).named(name);break;
                case "char" :
                case "varchar" :
                case "string" : typeBuilder.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(name);break;
                case "boolean" : typeBuilder.required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);break;
                case "timestamp" : typeBuilder.required(PrimitiveType.PrimitiveTypeName.INT96).named(name);break;
                case "date" :typeBuilder.required(PrimitiveType.PrimitiveTypeName.INT32).as(OriginalType.DATE).named(name);break;
                default:
                    if (colType.contains("decimal")){
                        int precision = Integer.parseInt(colType.substring(colType.indexOf("(") + 1,colType.indexOf(",")).trim());
                        int scale = Integer.parseInt(colType.substring(colType.indexOf(",") + 1,colType.indexOf(")")).trim());
                        typeBuilder.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(OriginalType.DECIMAL)
                                .precision(precision)
                                .scale(scale)
                                .named(name);
                    } else {
                        typeBuilder.required(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
                    }
                    break;
            }
        }

        return typeBuilder.named("Pair");
    }

    private int getDay(String dateStr) throws Exception{
        Date date = sdf.parse(dateStr);
        return (int)((date.getTime() - cal.getTimeInMillis()) / (1000 * 60 * 60 * 24));
    }
}
