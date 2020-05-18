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

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.hadoop.ParquetWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static com.dtstack.flinkx.hdfs.HdfsConfigKeys.*;

/**
 * The writer plugin of Hdfs
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsWriter extends BaseDataWriter {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected String defaultFs;

    protected String fileType;

    protected String path;

    protected String fieldDelimiter;

    protected String compress;

    protected String fileName;

    protected List<String> columnName;

    protected List<String> columnType;

    protected Map<String,Object> hadoopConfig;

    protected String charSet;

    protected List<String> fullColumnName;

    protected List<String> fullColumnType;

    protected int rowGroupSize;

    protected long maxFileSize;

    protected long flushInterval;

    public HdfsWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        hadoopConfig = (Map<String, Object>) writerConfig.getParameter().getVal(KEY_HADOOP_CONFIG);
        List columns = writerConfig.getParameter().getColumn();
        fileType = writerConfig.getParameter().getStringVal(KEY_FILE_TYPE);
        defaultFs = writerConfig.getParameter().getStringVal(KEY_DEFAULT_FS);
        path = writerConfig.getParameter().getStringVal(KEY_PATH);
        fieldDelimiter = writerConfig.getParameter().getStringVal(KEY_FIELD_DELIMITER);
        charSet = writerConfig.getParameter().getStringVal(KEY_ENCODING);
        rowGroupSize = writerConfig.getParameter().getIntVal(KEY_ROW_GROUP_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE);
        maxFileSize = writerConfig.getParameter().getLongVal(KEY_MAX_FILE_SIZE, 1024 * 1024 * 1024);
        flushInterval = writerConfig.getParameter().getLongVal(KEY_FLUSH_INTERVAL, 0);

        if(fieldDelimiter == null || fieldDelimiter.length() == 0) {
            fieldDelimiter = "\001";
        } else {
            fieldDelimiter = com.dtstack.flinkx.util.StringUtil.convertRegularExpr(fieldDelimiter);
        }

        compress = writerConfig.getParameter().getStringVal(KEY_COMPRESS);
        fileName = writerConfig.getParameter().getStringVal(KEY_FILE_NAME, "");
        if(CollectionUtils.isNotEmpty(columns)) {
            columnName = new ArrayList<>();
            columnType = new ArrayList<>();
            for (Object column : columns) {
                Map sm = (Map) column;
                columnName.add((String) sm.get(KEY_COLUMN_NAME));
                columnType.add((String) sm.get(KEY_COLUMN_TYPE));
            }
        }

        fullColumnName = (List<String>) writerConfig.getParameter().getVal(KEY_FULL_COLUMN_NAME_LIST);
        fullColumnType = (List<String>) writerConfig.getParameter().getVal(KEY_FULL_COLUMN_TYPE_LIST);

        mode = writerConfig.getParameter().getStringVal(KEY_WRITE_MODE);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        HdfsOutputFormatBuilder builder = new HdfsOutputFormatBuilder(fileType);
        builder.setHadoopConfig(hadoopConfig);
        builder.setDefaultFs(defaultFs);
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
        builder.setRowGroupSize(rowGroupSize);
        builder.setRestoreConfig(restoreConfig);
        builder.setMaxFileSize(maxFileSize);
        builder.setFlushBlockInterval(flushInterval);

        return createOutput(dataSet, builder.finish());
    }
}
