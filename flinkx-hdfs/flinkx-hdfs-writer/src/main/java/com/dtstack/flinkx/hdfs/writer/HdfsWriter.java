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

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static com.dtstack.flinkx.hdfs.HdfsConfigKeys.*;

/**
 * The writer plugin of Hdfs
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsWriter extends DataWriter {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected String defaultFS;

    protected String fileType;

    protected String path;

    protected String fieldDelimiter;

    protected String compress;

    protected String fileName;

    protected List<String> columnName;

    protected List<String> columnType;

    protected Map<String,String> hadoopConfig;

    protected String charSet;

    protected List<String> fullColumnName;

    protected List<String> fullColumnType;

    /** hive config **/
    protected String partition;

    protected String dbUrl;

    protected String username;

    protected String password;

    protected String table;

    protected static final String DATA_SUBDIR = ".data";

    protected static final String FINISHED_SUBDIR = ".finished";

    protected static final String SP = "/";

    public HdfsWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        hadoopConfig = (Map<String, String>) writerConfig.getParameter().getVal(KEY_HADOOP_CONFIG);
        List columns = writerConfig.getParameter().getColumn();
        fileType = writerConfig.getParameter().getStringVal(KEY_FILE_TYPE);
        defaultFS = writerConfig.getParameter().getStringVal(KEY_DEFAULT_FS);
        path = writerConfig.getParameter().getStringVal(KEY_PATH);
        fieldDelimiter = writerConfig.getParameter().getStringVal(KEY_FIELD_DELIMITER);
        charSet = writerConfig.getParameter().getStringVal(KEY_ENCODING);

        if(fieldDelimiter == null || fieldDelimiter.length() == 0) {
            fieldDelimiter = "\001";
        } else {
            fieldDelimiter = com.dtstack.flinkx.util.StringUtil.convertRegularExpr(fieldDelimiter);
        }

        compress = writerConfig.getParameter().getStringVal(KEY_COMPRESS);
        fileName = writerConfig.getParameter().getStringVal(KEY_FILE_NAME, "");
        if(columns != null || columns.size() != 0) {
            columnName = new ArrayList<>();
            columnType = new ArrayList<>();
            for(int i = 0; i < columns.size(); ++i) {
                Map sm = (Map) columns.get(i);
                columnName.add((String) sm.get(KEY_COLUMN_NAME));
                columnType.add((String) sm.get(KEY_COLUMN_TYPE));
            }
        }

        fullColumnName = (List<String>) writerConfig.getParameter().getVal(KEY_FULL_COLUMN_NAME_LIST);
        fullColumnType = (List<String>) writerConfig.getParameter().getVal(KEY_FULL_COLUMN_TYPE_LIST);

        mode = writerConfig.getParameter().getStringVal(KEY_WRITE_MODE);

        deleteTempDir();
    }

    public void deleteTempDir() throws RuntimeException{
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            if(hadoopConfig != null) {
                for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
                    conf.set(entry.getKey(), entry.getValue());
                }
            }

            conf.set("fs.default.name", defaultFS);
            conf.set("fs.hdfs.impl.disable.cache", "true");
            fs = FileSystem.get(conf);

            String outputFilePath;
            if(StringUtils.isNotBlank(fileName)) {
                outputFilePath = path + SP + fileName;
            } else {
                outputFilePath = path;
            }

            // delete tmp dir
            Path tmpDir = new Path(outputFilePath + SP + DATA_SUBDIR);
            Path finishedDir = new Path(outputFilePath + SP + FINISHED_SUBDIR);
            fs.delete(tmpDir, true);
            fs.delete(finishedDir, true);
        } catch (Exception e){
            LOG.error("delete temp dir error:",e);
            throw new RuntimeException(e);
        } finally {
            if (fs != null){
                try {
                    fs.close();
                } catch (Exception e){
                    LOG.error("close filesystem error:",e);
                }
            }
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        HdfsOutputFormatBuilder builder = new HdfsOutputFormatBuilder(fileType);
        builder.setHadoopConfig(hadoopConfig);
        builder.setDefaultFS(defaultFS);
        builder.setPath(path);
        builder.setFileName(fileName);
        builder.setWriteMode(mode);
        builder.setColumnNames(columnName);
        builder.setColumnTypes(columnType);
        builder.setCompress(compress);
        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setErrorRatio(errorRatio);
        builder.setFullColumnNames(fullColumnName);
        builder.setFullColumnTypes(fullColumnType);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setCharSetName(charSet);
        builder.setDelimiter(fieldDelimiter);

        OutputFormatSinkFunction sinkFunction = new OutputFormatSinkFunction(builder.finish());
        DataStreamSink<?> dataStreamSink = dataSet.addSink(sinkFunction);

        dataStreamSink.name("hdfswriter");

        return dataStreamSink;
    }
}
