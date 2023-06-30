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
package com.dtstack.chunjun.connector.hdfs.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.hdfs.InputSplit.HdfsParquetSplit;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.FileSystemUtil;
import com.dtstack.chunjun.util.PluginUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.InputFormat;
import parquet.example.data.Group;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.io.api.Binary;
import parquet.schema.DecimalMetadata;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.PrivilegedAction;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HdfsParquetInputFormat extends BaseHdfsInputFormat {

    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    private static final int TIMESTAMP_BINARY_LENGTH = 12;
    private static final long serialVersionUID = 8835924162626158151L;

    private transient Group currentLine;
    private transient ParquetReader<Group> currentFileReader;
    private transient List<String> fullColNames;
    private transient List<TypeConfig> fullColTypes;
    private transient List<String> currentSplitFilePaths;
    private transient int currentFileIndex = 0;

    private static List<String> getAllPartitionPath(
            String tableLocation, FileSystem fs, PathFilter pathFilter) throws IOException {
        List<String> pathList = Lists.newArrayList();
        Path inputPath = new Path(tableLocation);

        if (fs.getFileStatus(inputPath).isFile()) {
            pathList.add(tableLocation);
            return pathList;
        }

        FileStatus[] fsStatus = fs.listStatus(inputPath, pathFilter);
        for (FileStatus status : fsStatus) {
            pathList.addAll(getAllPartitionPath(status.getPath().toString(), fs, pathFilter));
        }

        return pathList;
    }

    @Override
    public InputSplit[] createHdfsSplit(int minNumSplits) {
        List<String> allFilePaths;
        HdfsPathFilter pathFilter = new HdfsPathFilter(hdfsConfig.getFilterRegex());

        try (FileSystem fs =
                FileSystemUtil.getFileSystem(
                        hdfsConfig.getHadoopConfig(),
                        hdfsConfig.getDefaultFS(),
                        PluginUtil.createDistributedCacheFromContextClassLoader(),
                        jobId,
                        String.valueOf(indexOfSubTask))) {
            allFilePaths = getAllPartitionPath(hdfsConfig.getPath(), fs, pathFilter);
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }

        if (allFilePaths.size() > 0) {
            HdfsParquetSplit[] splits = new HdfsParquetSplit[minNumSplits];
            for (int i = 0; i < minNumSplits; i++) {
                splits[i] = new HdfsParquetSplit(i, new ArrayList<>());
            }

            Iterator<String> it = allFilePaths.iterator();
            while (it.hasNext()) {
                for (HdfsParquetSplit split : splits) {
                    if (it.hasNext()) {
                        split.getPaths().add(it.next());
                    }
                }
            }

            return splits;
        }

        return new HdfsParquetSplit[0];
    }

    @Override
    public InputFormat createInputFormat() {
        return null;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        currentSplitFilePaths = ((HdfsParquetSplit) inputSplit).getPaths();
    }

    private void getNextLine() throws IOException {
        if (currentFileReader != null) {
            if (openKerberos) {
                currentLine = nextLineWithKerberos();
            } else {
                currentLine = currentFileReader.read();
            }
        }
    }

    private boolean nextLine() throws IOException {
        getNextLine();
        if (currentLine != null) {
            setMetaColumns();
            return true;
        }
        while (currentFileIndex <= currentSplitFilePaths.size() - 1) {
            if (openKerberos) {
                ugi.doAs(
                        (PrivilegedAction<Object>)
                                () -> {
                                    try {
                                        nextFile();
                                        return null;
                                    } catch (IOException e) {
                                        throw new ChunJunRuntimeException(e);
                                    }
                                });
            } else {
                nextFile();
            }
            getNextLine();
            if (currentLine != null) {
                setMetaColumns();
                return true;
            }
        }
        return false;
    }

    private void setMetaColumns() {
        if (fullColNames == null && currentLine != null) {
            fullColNames = new ArrayList<>();
            fullColTypes = new ArrayList<>();
            List<Type> types = currentLine.getType().getFields();
            for (Type type : types) {
                fullColNames.add(type.getName().toUpperCase());
                fullColTypes.add(
                        getTypeName(type.asPrimitiveType().getPrimitiveTypeName().getMethod));
            }

            for (FieldConfig fieldConfig : hdfsConfig.getColumn()) {
                String name = fieldConfig.getName();
                if (StringUtils.isNotBlank(name)) {
                    name = name.toUpperCase();
                    if (fullColNames.contains(name)) {
                        fieldConfig.setIndex(fullColNames.indexOf(name));
                    } else {
                        fieldConfig.setIndex(-1);
                    }
                }
            }
        }
    }

    private Group nextLineWithKerberos() {
        return ugi.doAs(
                (PrivilegedAction<Group>)
                        () -> {
                            try {
                                return currentFileReader.read();
                            } catch (IOException e) {
                                throw new ChunJunRuntimeException(e);
                            }
                        });
    }

    private void nextFile() throws IOException {
        Path path = new Path(currentSplitFilePaths.get(currentFileIndex));
        findCurrentPartition(path);
        ParquetReader.Builder<Group> reader =
                ParquetReader.builder(new GroupReadSupport(), path).withConf(hadoopJobConf);
        currentFileReader = reader.build();
        currentFileIndex++;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        List<FieldConfig> fieldConfList = hdfsConfig.getColumn();
        GenericRowData genericRowData;
        if (fieldConfList.size() == 1
                && ConstantValue.STAR_SYMBOL.equals(fieldConfList.get(0).getName())) {
            genericRowData = new GenericRowData(fullColNames.size());
            for (int i = 0; i < fullColNames.size(); i++) {
                Object obj = getData(currentLine, fullColTypes.get(i), i);
                genericRowData.setField(i, obj);
            }
        } else {
            genericRowData = new GenericRowData(fieldConfList.size());
            for (int i = 0; i < fieldConfList.size(); i++) {
                FieldConfig fieldConfig = fieldConfList.get(i);
                Object obj = null;
                if (fieldConfig.getValue() != null) {
                    obj = fieldConfig.getValue();
                } else if (fieldConfig.getIndex() != null
                        && fieldConfig.getIndex() < fullColNames.size()) {
                    if (currentLine.getFieldRepetitionCount(fieldConfig.getIndex()) > 0) {
                        obj = getData(currentLine, fieldConfig.getType(), fieldConfig.getIndex());
                    }
                }

                genericRowData.setField(i, obj);
            }
        }

        try {
            return rowConverter.toInternal(genericRowData);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !nextLine();
    }

    public Object getData(Group currentLine, TypeConfig type, int index) {
        Object data = null;
        ColumnType columnType = ColumnType.fromString(type.getType());

        try {
            if (index == -1) {
                return null;
            }

            Type colSchemaType = currentLine.getType().getType(index);
            switch (columnType.name().toLowerCase(Locale.ENGLISH)) {
                case "tinyint":
                case "smallint":
                case "int":
                    data = currentLine.getInteger(index, 0);
                    break;
                case "bigint":
                    data = currentLine.getLong(index, 0);
                    break;
                case "float":
                    data = currentLine.getFloat(index, 0);
                    break;
                case "double":
                    data = currentLine.getDouble(index, 0);
                    break;
                case "binary":
                    Binary binaryData = currentLine.getBinary(index, 0);
                    data = binaryData.getBytes();
                    break;
                case "char":
                case "varchar":
                case "string":
                    data = currentLine.getString(index, 0);
                    break;
                case "boolean":
                    data = currentLine.getBoolean(index, 0);
                    break;
                case "timestamp":
                    {
                        long time = getTimestampMillis(currentLine.getInt96(index, 0));
                        data = new Timestamp(time);
                        break;
                    }
                case "decimal":
                    {
                        DecimalMetadata dm = ((PrimitiveType) colSchemaType).getDecimalMetadata();
                        String primitiveTypeName =
                                currentLine
                                        .getType()
                                        .getType(index)
                                        .asPrimitiveType()
                                        .getPrimitiveTypeName()
                                        .name();
                        if (ColumnType.INT32.name().equals(primitiveTypeName)) {
                            int intVal = currentLine.getInteger(index, 0);
                            data = longToDecimalStr(intVal, dm.getScale());
                        } else if (ColumnType.INT64.name().equals(primitiveTypeName)) {
                            long longVal = currentLine.getLong(index, 0);
                            data = longToDecimalStr(longVal, dm.getScale());
                        } else {
                            Binary binary = currentLine.getBinary(index, 0);
                            data = binaryToDecimalStr(binary, dm.getScale());
                        }
                        break;
                    }
                case "date":
                    {
                        String val = currentLine.getValueToString(index, 0);
                        data =
                                new Timestamp(Integer.parseInt(val) * MILLIS_IN_DAY)
                                        .toString()
                                        .substring(0, 10);
                        break;
                    }
                default:
                    data = currentLine.getValueToString(index, 0);
                    break;
            }
        } catch (Exception e) {
            log.error("error to get data from parquet group.", e);
        }

        return data;
    }

    @Override
    public void closeInternal() throws IOException {
        if (currentFileReader != null) {
            currentFileReader.close();
            currentFileReader = null;
        }

        currentLine = null;
        currentFileIndex = 0;
    }

    private BigDecimal longToDecimalStr(long value, int scale) {
        BigInteger bi = BigInteger.valueOf(value);
        return new BigDecimal(bi, scale);
    }

    private BigDecimal binaryToDecimalStr(Binary binary, int scale) {
        BigInteger bi = new BigInteger(binary.getBytes());
        return new BigDecimal(bi, scale);
    }

    private TypeConfig getTypeName(String method) {
        String typeName;
        switch (method) {
            case "getBoolean":
            case "getInteger":
                typeName = "int";
                break;
            case "getInt96":
                typeName = "bigint";
                break;
            case "getFloat":
                typeName = "float";
                break;
            case "getDouble":
                typeName = "double";
                break;
            case "getBinary":
                typeName = "binary";
                break;
            default:
                typeName = "string";
        }

        return TypeConfig.fromString(typeName);
    }

    private long getTimestampMillis(Binary timestampBinary) {
        if (timestampBinary.length() != TIMESTAMP_BINARY_LENGTH) {
            return 0;
        }

        byte[] bytes = timestampBinary.getBytes();

        long timeOfDayNanos =
                Longs.fromBytes(
                        bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1],
                        bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
    }

    private long julianDayToMillis(int julianDay) {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }
}
