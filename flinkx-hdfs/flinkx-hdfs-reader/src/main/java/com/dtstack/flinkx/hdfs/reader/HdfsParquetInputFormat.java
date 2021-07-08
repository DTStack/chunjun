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

package com.dtstack.flinkx.hdfs.reader;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.hdfs.HdfsUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.FileSystemUtil;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The subclass of HdfsInputFormat which handles parquet files
 *
 * Company: www.dtstack.com
 * @author jiangbo
 */
public class HdfsParquetInputFormat extends HdfsInputFormat {

    private transient Group currentLine;

    private transient ParquetReader<Group> currentFileReader;

    private transient List<String> allFilePaths;

    private transient List<String> fullColNames;

    private transient List<String> fullColTypes;

    private transient List<String> currentSplitFilePaths;

    private transient int currentFileIndex = 0;

    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;

    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

    private static final String EXCLUDE_FILE = "_SUCCESS";

    @Override
    protected void configureAnythingElse() {
        FileSystem fs = null;
        try {
            fs = FileSystemUtil.getFileSystem(hadoopConfig, defaultFS, jobId, "reader");
            allFilePaths = getAllPartitionPath(inputPath, fs);
        } catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            if(fs != null){
                try {
                    fs.close();
                } catch (IOException e) {
                    throw new RuntimeException("Close FileSystem error after get all files", e);
                }
            }
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        currentSplitFilePaths = ((HdfsParquetSplit)inputSplit).getPaths();
    }

    private boolean nextLine() throws IOException{
        if (currentFileReader == null && currentFileIndex <= currentSplitFilePaths.size()-1){
            nextFile();
        }

        if (currentFileReader == null){
            return false;
        }

        currentLine = currentFileReader.read();
        if (fullColNames == null && currentLine != null){
            fullColNames = new ArrayList<>();
            fullColTypes = new ArrayList<>();
            List<Type> types = currentLine.getType().getFields();
            for (Type type : types) {
                fullColNames.add(type.getName().toUpperCase());
                fullColTypes.add(getTypeName(type.asPrimitiveType().getPrimitiveTypeName().getMethod));
            }

            for (MetaColumn metaColumn : metaColumns) {
                if(fullColNames.contains(metaColumn.getName().toUpperCase())){
                    metaColumn.setIndex(fullColNames.indexOf(metaColumn.getName().toUpperCase()));
                } else {
                    metaColumn.setIndex(-1);
                }
            }
        }

        if (currentLine == null){
            currentFileReader = null;
            nextLine();
        }

        return currentLine != null;
    }

    private void nextFile() throws IOException{
        String path = currentSplitFilePaths.get(currentFileIndex);
        ParquetReader.Builder<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(path)).withConf(conf);
        currentFileReader = reader.build();

        currentFileIndex++;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        if(metaColumns.size() == 1 && "*".equals(metaColumns.get(0).getName())){
            row = new Row(fullColNames.size());
            for (int i = 0; i < fullColNames.size(); i++) {
                Object val = getData(currentLine,fullColTypes.get(i),i);
                row.setField(i, val);
            }
        } else {
            row = new Row(metaColumns.size());
            for (int i = 0; i < metaColumns.size(); i++) {
                MetaColumn metaColumn = metaColumns.get(i);
                Object val = null;

                if(metaColumn.getIndex() != -1){
                    if(currentLine.getFieldRepetitionCount(metaColumn.getName()) > 0){
                        val = getData(currentLine,metaColumn.getType(),metaColumn.getIndex());
                    }

                    if (val == null && metaColumn.getValue() != null){
                        val = metaColumn.getValue();
                    }
                } else if (metaColumn.getValue() != null){
                    val = metaColumn.getValue();
                }

                if(val instanceof String){
                    val = HdfsUtil.string2col(String.valueOf(val),metaColumn.getType(),metaColumn.getTimeFormat());
                }

                row.setField(i,val);
            }
        }

        return row;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !nextLine();
    }

    private Object getData(Group currentLine,String type,int index){
        Object data = null;
        ColumnType columnType = ColumnType.fromString(type);

        try{
            if (index == -1){
                return null;
            }

            Type colSchemaType = currentLine.getType().getType(index);
            switch (columnType.name().toLowerCase()){
                case "tinyint" :
                case "smallint" :
                case "int" : data = currentLine.getInteger(index,0);break;
                case "bigint" : data = currentLine.getLong(index,0);break;
                case "float" : data = currentLine.getFloat(index,0);break;
                case "double" : data = currentLine.getDouble(index,0);break;
                case "binary" :data = currentLine.getBinary(index,0);break;
                case "char" :
                case "varchar" :
                case "string" : data = currentLine.getString(index,0);break;
                case "boolean" : data = currentLine.getBoolean(index,0);break;
                case "timestamp" :{
                    long time = getTimestampMillis(currentLine.getInt96(index,0));
                    data = new Timestamp(time);
                    break;
                }
                case "decimal" : {
                    DecimalMetadata dm = ((PrimitiveType) colSchemaType).getDecimalMetadata();
                    String primitiveTypeName = currentLine.getType().getType(index).asPrimitiveType().getPrimitiveTypeName().name();
                    if ("INT32".equals(primitiveTypeName)){
                        int intVal = currentLine.getInteger(index,0);
                        data = longToDecimalStr((long)intVal,dm.getScale());
                    } else if("INT64".equals(primitiveTypeName)){
                        long longVal = currentLine.getLong(index,0);
                        data = longToDecimalStr(longVal,dm.getScale());
                    } else {
                        Binary binary = currentLine.getBinary(index,0);
                        data = binaryToDecimalStr(binary,dm.getScale());
                    }
                    break;
                }
                case "date" : {
                    String val = currentLine.getValueToString(index,0);
                    data = new Timestamp(Integer.parseInt(val) * 60 * 60 * 24 * 1000L).toString().substring(0,10);
                    break;
                }
                default: data = currentLine.getValueToString(index,0);break;
            }
        } catch (Exception e){
            LOG.error("{}",e);
        }

        return data;
    }

    @Override
    public HdfsParquetSplit[] createInputSplits(int minNumSplits) throws IOException {
        if(allFilePaths != null && allFilePaths.size() > 0){
            HdfsParquetSplit[] splits = new HdfsParquetSplit[minNumSplits];
            for (int i = 0; i < minNumSplits; i++) {
                splits[i] = new HdfsParquetSplit(i, new ArrayList<>());
            }

            Iterator<String> it = allFilePaths.iterator();
            while (it.hasNext()) {
                for (HdfsParquetSplit split : splits) {
                    if (it.hasNext()){
                        split.getPaths().add(it.next());
                    }
                }
            }

            return splits;
        }

        return new HdfsParquetSplit[0];
    }

    @Override
    public void closeInternal() throws IOException {
        if (currentFileReader != null){
            currentFileReader.close();
            currentFileReader = null;
        }

        currentLine = null;
        currentFileIndex = 0;
    }

    private String longToDecimalStr(long value,int scale){
        BigInteger bi = BigInteger.valueOf(value);
        BigDecimal bg = new BigDecimal(bi, scale);

        return bg.toString();
    }

    private String binaryToDecimalStr(Binary binary,int scale){
        BigInteger bi = new BigInteger(binary.getBytes());
        BigDecimal bg = new BigDecimal(bi,scale);

        return bg.toString();
    }

    private static List<String> getAllPartitionPath(String tableLocation, FileSystem fs) throws IOException {
        List<String> pathList = Lists.newArrayList();
        Path inputPath = new Path(tableLocation);

        if(fs.isFile(inputPath) && !inputPath.getName().equals(EXCLUDE_FILE)){
            pathList.add(tableLocation);
            return pathList;
        } else {
            FileStatus[] fsStatus = fs.listStatus(inputPath, path -> !path.getName().startsWith("."));
            for (FileStatus status : fsStatus) {
                if(status.isDirectory()){
                    pathList.addAll(getAllPartitionPath(status.getPath().toString(), fs));
                } else if(!status.getPath().getName().equals(EXCLUDE_FILE)){
                    pathList.add(status.getPath().toString());
                }
            }
        }

        return pathList;
    }

    private String getTypeName(String method){
        String typeName;
        switch (method){
            case "getInteger" : typeName = "int";break;
            case "getInt96" : typeName = "bigint";break;
            case "getFloat" : typeName = "float";break;
            case "getDouble" : typeName = "double";break;
            case "getBinary" : typeName = "binary";break;
            case "getString" : typeName = "string";break;
            case "getBoolean" : typeName = "int";break;
            default:typeName = "string";
        }

        return typeName;
    }

    /**
     * @param timestampBinary
     * @return
     */
    private long getTimestampMillis(Binary timestampBinary)
    {
        if (timestampBinary.length() != 12) {
            return 0;
        }
        byte[] bytes = timestampBinary.getBytes();

        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
    }

    private long julianDayToMillis(int julianDay)
    {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }

    static class HdfsParquetSplit implements InputSplit{

        private int splitNumber;

        private List<String> paths;

        public HdfsParquetSplit(int splitNumber, List<String> paths) {
            this.splitNumber = splitNumber;
            this.paths = paths;
        }

        @Override
        public int getSplitNumber() {
            return splitNumber;
        }

        public List<String> getPaths() {
            return paths;
        }
    }
}
