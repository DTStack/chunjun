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

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.hdfs.HdfsConfigKeys;
import com.dtstack.flinkx.reader.DataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The reader plugin of Hdfs
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsReader extends DataReader {
    protected String type;
    protected String defaultFS;
    protected String fileType;
    protected String path;
    protected String fieldDelimiter;
    protected List<Integer> columnIndex;
    protected List<String> columnType;
    protected List<String> columnValue;
    protected List<String> columnName;
    protected Map<String,String> hadoopConfig;

    public HdfsReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        defaultFS = readerConfig.getParameter().getStringVal(HdfsConfigKeys.KEY_DEFAULT_FS);
        path = readerConfig.getParameter().getStringVal(HdfsConfigKeys.KEY_PATH);
        fileType = readerConfig.getParameter().getStringVal(HdfsConfigKeys.KEY_FILE_TYPE);
        hadoopConfig = (Map<String, String>) readerConfig.getParameter().getVal(HdfsConfigKeys.KEY_HADOOP_CONFIG);

        fieldDelimiter = readerConfig.getParameter().getStringVal(HdfsConfigKeys.KEY_FIELD_DELIMITER);

        if(fieldDelimiter == null || fieldDelimiter.length() == 0) {
            fieldDelimiter = "\001";
        } else {
            String pattern = "\\\\(\\d{3})";

            Pattern r = Pattern.compile(pattern);
            while(true) {
                Matcher m = r.matcher(fieldDelimiter);
                if(!m.find()) {
                    break;
                }
                String num = m.group(1);
                int x = Integer.parseInt(num, 8);
                fieldDelimiter = m.replaceFirst(String.valueOf((char)x));
            }
            fieldDelimiter = fieldDelimiter.replaceAll("\\\\t","\t");
            fieldDelimiter = fieldDelimiter.replaceAll("\\\\r","\r");
            fieldDelimiter = fieldDelimiter.replaceAll("\\\\n","\n");
        }

        List columns = readerConfig.getParameter().getColumn();
        if(columns != null && columns.size() > 0) {
            if(columns.get(0) instanceof Map) {
                columnIndex = new ArrayList<>();
                columnType = new ArrayList<>();
                columnValue = new ArrayList<>();
                columnName = new ArrayList<>();
                for(int i = 0; i < columns.size(); ++i) {
                    Map sm = (Map) columns.get(i);
                    Double temp = (Double)sm.get("index");
                    columnIndex.add(temp != null ? temp.intValue() : null);
                    columnType.add((String) sm.get("type"));
                    columnValue.add((String) sm.get("value"));
                    columnName.add((String) sm.get("name"));
                }
                System.out.println("init column finished");
            } else if (!columns.get(0).equals("*") || columns.size() != 1) {
                throw new IllegalArgumentException("column argument error");
            }
        } else{
            throw new IllegalArgumentException("column argument error");
        }
    }

    @Override
    public DataStream<Row> readData() {
        HdfsInputFormatBuilder builder = new HdfsInputFormatBuilder(fileType);
        builder.setInputPaths(path);
        builder.setColumnIndex(columnIndex);
        builder.setColumnName(columnName);
        builder.setColumnType(columnType);
        builder.setColumnValue(columnValue);
        builder.setHadoopConfig(hadoopConfig);
        builder.setDefaultFs(defaultFS);
        builder.setDelimiter(fieldDelimiter);
        builder.setBytes(bytes);
        builder.setMonitorUrls(monitorUrls);

        return createInput(builder.finish(), "hdfsreader");
    }

}
