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
package com.dtstack.flinkx.connector.hdfs.sink;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hdfs.converter.HdfsParquetColumnConverter;
import com.dtstack.flinkx.connector.hdfs.converter.HdfsParquetRowConverter;
import com.dtstack.flinkx.connector.hdfs.enums.CompressType;
import com.dtstack.flinkx.connector.hdfs.enums.FileType;
import com.dtstack.flinkx.connector.hdfs.util.HdfsUtil;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.enums.SizeUnitType;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.WriteRecordException;
import com.dtstack.flinkx.util.ColumnTypeUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.FileSystemUtil;

import org.apache.flink.table.data.RowData;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Date: 2021/06/20 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HdfsParquetOutputFormat extends BaseHdfsOutputFormat {

    private static final ColumnTypeUtil.DecimalInfo PARQUET_DEFAULT_DECIMAL_INFO =
            new ColumnTypeUtil.DecimalInfo(10, 0);
    private SimpleGroupFactory groupFactory;
    private ParquetWriter<Group> writer;
    private MessageType schema;

    @Override
    protected void openSource() {
        super.openSource();

        schema = buildSchema();
        GroupWriteSupport.setSchema(schema, conf);
        groupFactory = new SimpleGroupFactory(schema);
        List<String> columnNameList =
                hdfsConf.getColumn().stream().map(FieldConf::getName).collect(Collectors.toList());
        if (rowConverter instanceof HdfsParquetColumnConverter) {
            ((HdfsParquetColumnConverter) rowConverter).setColumnNameList(columnNameList);
            ((HdfsParquetColumnConverter) rowConverter).setDecimalColInfo(decimalColInfo);
        } else if (rowConverter instanceof HdfsParquetRowConverter) {
            ((HdfsParquetRowConverter) rowConverter).setColumnNameList(columnNameList);
        }
    }

    @Override
    protected void nextBlock() {
        super.nextBlock();

        if (writer != null) {
            return;
        }

        try {
            String currentBlockTmpPath = tmpPath + File.separatorChar + currentFileName;
            Path writePath = new Path(currentBlockTmpPath);

            // Compatible with old code
            CompressionCodecName compressionCodecName;
            switch (compressType) {
                case PARQUET_SNAPPY:
                    compressionCodecName = CompressionCodecName.SNAPPY;
                    break;
                case PARQUET_GZIP:
                    compressionCodecName = CompressionCodecName.GZIP;
                    break;
                case PARQUET_LZO:
                    compressionCodecName = CompressionCodecName.LZO;
                    break;
                default:
                    compressionCodecName = CompressionCodecName.UNCOMPRESSED;
            }

            ExampleParquetWriter.Builder builder =
                    ExampleParquetWriter.builder(writePath)
                            .withWriteMode(ParquetFileWriter.Mode.CREATE)
                            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                            .withCompressionCodec(compressionCodecName)
                            .withConf(conf)
                            .withType(schema)
                            .withDictionaryEncoding(hdfsConf.isEnableDictionary())
                            .withRowGroupSize(hdfsConf.getRowGroupSize());

            // 开启kerberos 需要在ugi里进行build
            if (FileSystemUtil.isOpenKerberos(hdfsConf.getHadoopConfig())) {
                UserGroupInformation ugi =
                        FileSystemUtil.getUGI(
                                hdfsConf.getHadoopConfig(),
                                hdfsConf.getDefaultFS(),
                                getRuntimeContext().getDistributedCache());
                ugi.doAs(
                        (PrivilegedAction<Object>)
                                () -> {
                                    try {
                                        writer = builder.build();
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return null;
                                });
            } else {
                writer = builder.build();
            }
            currentFileIndex++;
        } catch (Exception e) {
            throw new FlinkxRuntimeException(
                    HdfsUtil.parseErrorMsg(null, ExceptionUtil.getErrorMessage(e)), e);
        }
    }

    @Override
    public void flushDataInternal() {
        LOG.info(
                "Close current parquet record writer, write data size:[{}]",
                SizeUnitType.readableFileSize(bytesWriteCounter.getLocalValue()));
        try {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        } catch (IOException e) {
            throw new FlinkxRuntimeException(
                    HdfsUtil.parseErrorMsg(
                            "error to flush stream.", ExceptionUtil.getErrorMessage(e)),
                    e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeSingleRecordToFile(RowData rowData) throws WriteRecordException {
        if (writer == null) {
            nextBlock();
        }

        Group group = groupFactory.newGroup();
        try {
            group = (Group) rowConverter.toExternal(rowData, group);
        } catch (Exception e) {
            String errorMessage =
                    HdfsUtil.parseErrorMsg(
                            String.format("writer hdfs error，rowData:{%s}", rowData),
                            ExceptionUtil.getErrorMessage(e));
            throw new WriteRecordException(errorMessage, e, -1, rowData);
        }

        try {
            writer.write(group);
            rowsOfCurrentBlock++;
            lastRow = rowData;
        } catch (IOException e) {
            throw new WriteRecordException(
                    String.format("Data writing to hdfs is abnormal，rowData:{%s}", rowData), e);
        }
    }

    @Override
    protected void closeSource() {
        try {
            LOG.info("close:Current block writer record:" + rowsOfCurrentBlock);
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            throw new FlinkxRuntimeException("close stream error.", e);
        } finally {
            super.closeSource();
        }
    }

    @Override
    public CompressType getCompressType() {
        return CompressType.getByTypeAndFileType(hdfsConf.getCompress(), FileType.PARQUET.name());
    }

    @SuppressWarnings("all")
    private MessageType buildSchema() {
        decimalColInfo = new HashMap<>(16);
        Types.MessageTypeBuilder typeBuilder = Types.buildMessage();
        for (int i = 0; i < fullColumnNameList.size(); i++) {
            String name = fullColumnNameList.get(i);
            String colType = fullColumnTypeList.get(i).toLowerCase(Locale.ENGLISH);
            int left = colType.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
            int right = colType.indexOf(ConstantValue.RIGHT_PARENTHESIS_SYMBOL);
            if (left > 0 && right > 0) {
                colType = colType.substring(0, left);
            }
            switch (colType) {
                case "tinyint":
                case "smallint":
                case "int":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(name);
                    break;
                case "bigint":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name);
                    break;
                case "float":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(name);
                    break;
                case "double":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
                    break;
                case "binary":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
                    break;
                case "char":
                case "varchar":
                case "string":
                    typeBuilder
                            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                            .as(OriginalType.UTF8)
                            .named(name);
                    break;
                case "boolean":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);
                    break;
                case "timestamp":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT96).named(name);
                    break;
                case "date":
                    typeBuilder
                            .optional(PrimitiveType.PrimitiveTypeName.INT32)
                            .as(OriginalType.DATE)
                            .named(name);
                    break;
                default:
                    if (ColumnTypeUtil.isDecimalType(colType)) {
                        ColumnTypeUtil.DecimalInfo decimalInfo =
                                ColumnTypeUtil.getDecimalInfo(
                                        colType, PARQUET_DEFAULT_DECIMAL_INFO);
                        typeBuilder
                                .optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                .as(OriginalType.DECIMAL)
                                .precision(decimalInfo.getPrecision())
                                .scale(decimalInfo.getScale())
                                .length(
                                        HdfsUtil.computeMinBytesForPrecision(
                                                decimalInfo.getPrecision()))
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
