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

import com.dtstack.flinkx.hdfs.HdfsUtil;
import com.google.common.collect.Lists;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

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

    private transient List<String> currentSplitFilePaths;

    private transient int currenFileIndex = 0;

    private GroupReadSupport readSupport;

    private SimpleDateFormat sdf = new SimpleDateFormat("");

    @Override
    protected void configureAnythingElse() {
        try {
            readSupport = new GroupReadSupport();
            allFilePaths = getAllPartitionPath(inputPath);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        currentSplitFilePaths = ((HdfsParquetSplit)inputSplit).getPaths();
    }

    private boolean nextLine() throws IOException{
        if (currentFileReader == null && currenFileIndex <= currentSplitFilePaths.size()-1){
            nextFile();
        }

        if (currentFileReader == null){
            return false;
        }

        currentLine = currentFileReader.read();

        if (currentLine == null){
            currentFileReader = null;
            nextLine();
        }

        return currentLine != null;
    }

    private void nextFile() throws IOException{
        String path = currentSplitFilePaths.get(currenFileIndex);
        ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, new Path(path)).withConf(conf);
        currentFileReader = reader.build();

        currenFileIndex++;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        row = new Row(columnIndex.size());
        Object col;
        for (int i = 0; i < columnIndex.size(); i++) {
            Integer index = columnIndex.get(i);
            String staticVal = columnValue.get(i);
            String type = columnType.get(i);
            if(index != null){
                col = getDate(currentLine,type,index);
                row.setField(i, col);
            } else {
                if(staticVal != null){
                    row.setField(i, HdfsUtil.string2col(staticVal,type));
                } else {
                    col = getDate(currentLine,type,currentLine.getType().getFieldIndex(columnName.get(i)));
                    row.setField(i, col);
                }
            }
        }

        return row;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !nextLine();
    }

    private Object getDate(Group currentLine,String type,int index){
        Object data;
        switch (type){
            case "tinyint" :
            case "smallint" :
            case "int" : data = currentLine.getInteger(index,0);break;
            case "bigint" : data = currentLine.getInt96(index,0);break;
            case "float" : data = currentLine.getFloat(index,0);break;
            case "double" : data = currentLine.getDouble(index,0);break;
            case "binary" :data = currentLine.getBinary(index,0);break;
            case "char" :
            case "varchar" :
            case "string" : data = currentLine.getString(index,0);break;
            case "boolean" : data = currentLine.getBoolean(index,0);break;
            case "timestamp" :{
                String val = currentLine.getValueToString(index,0);
                data = new Timestamp(Long.parseLong(val));
                break;
            }
            case "decimal" : {
                String val = currentLine.getValueToString(index,0);
                data = Double.parseDouble(val);
                break;
            }
            case "date" : {
                String val = currentLine.getValueToString(index,0);
                try{
                    data = sdf.parse(val);
                } catch (ParseException pe){
                    data = val;
                }
                break;
            }
            default: data = currentLine.getValueToString(index,0);break;
        }

        return data;
    }

    @Override
    public HdfsParquetSplit[] createInputSplits(int minNumSplits) throws IOException {
        if(allFilePaths != null && allFilePaths.size() > 0){
            int step = allFilePaths.size() / minNumSplits;
            HdfsParquetSplit[] splits = new HdfsParquetSplit[minNumSplits];
            for (int i = 0; i < minNumSplits; i++) {
                int start = i * step;
                int end = (i+1) * step > allFilePaths.size() ? allFilePaths.size() : (i+1) * step;
                splits[i] = new HdfsParquetSplit(i,new ArrayList<>(allFilePaths.subList(start,end)));
            }

            return splits;
        }

        return null;
    }

    @Override
    public void closeInternal() throws IOException {
        if (currentFileReader != null){
            currentFileReader.close();
        }
    }

    private List<String> getAllPartitionPath(String tableLocation) throws IOException {
        FileSystem fs = null;
        List<String> pathList = Lists.newArrayList();
        try {
            Path inputPath = new Path(tableLocation);
            fs =  FileSystem.get(conf);

            FileStatus[] fsStatus = fs.listStatus(inputPath, path -> !path.getName().startsWith("."));
            if(fsStatus == null || fsStatus.length == 0){
                pathList.add(tableLocation);
                return pathList;
            }

            if(fsStatus[0].isDirectory()){
                for(FileStatus status : fsStatus){
                    pathList.addAll(getAllPartitionPath(status.getPath().toString()));
                }
                return pathList;
            }else{
                pathList.add(tableLocation);
                return pathList;
            }
        } finally {
            if (fs != null){
                fs.close();
            }
        }
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
