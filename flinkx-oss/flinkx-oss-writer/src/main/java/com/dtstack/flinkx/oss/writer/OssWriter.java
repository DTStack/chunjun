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
package com.dtstack.flinkx.oss.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.oss.util.StrUtil;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.oss.OssConfigKeys.*;

/**
 * The writer plugin of oss
 *
 * @author wangyulei
 * @date 2021-06-29
 */
public class OssWriter extends BaseDataWriter {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected String fileType;

    protected String path;

    protected String fieldDelimiter;

    protected String compress;

    protected String fileName;

    protected List<String> columnName;

    protected List<String> columnType;

    protected String endpoint;

    protected String accessKey;

    protected String secretKey;

    protected String charSet;

    protected List<String> fullColumnName;

    protected List<String> fullColumnType;

    protected int rowGroupSize;

    protected long maxFileSize;

    protected long flushInterval;

    protected boolean enableDictionary;

    public OssWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        endpoint = writerConfig.getParameter().getStringVal(KEY_ENDPOINT);
        accessKey = writerConfig.getParameter().getStringVal(KEY_ACCESS_KEY);
        secretKey = writerConfig.getParameter().getStringVal(KEY_SECRET_KEY);
        List columns = writerConfig.getParameter().getColumn();
        fileType = writerConfig.getParameter().getStringVal(KEY_FILE_TYPE);

        path = writerConfig.getParameter().getStringVal(KEY_PATH);
        fieldDelimiter = writerConfig.getParameter().getStringVal(KEY_FIELD_DELIMITER);
        charSet = writerConfig.getParameter().getStringVal(KEY_ENCODING);
        rowGroupSize = writerConfig.getParameter().getIntVal(KEY_ROW_GROUP_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE);
        maxFileSize = writerConfig.getParameter().getLongVal(KEY_MAX_FILE_SIZE, ConstantValue.STORE_SIZE_G);
        flushInterval = writerConfig.getParameter().getLongVal(KEY_FLUSH_INTERVAL, 0);
        enableDictionary = writerConfig.getParameter().getBooleanVal(KEY_ENABLE_DICTIONARY, true);

        if (fieldDelimiter == null || fieldDelimiter.length() == 0) {
            fieldDelimiter = "\001";
        } else {
            fieldDelimiter = com.dtstack.flinkx.util.StringUtil.convertRegularExpr(fieldDelimiter);
        }

        compress = writerConfig.getParameter().getStringVal(KEY_COMPRESS);
        fileName = writerConfig.getParameter().getStringVal(KEY_FILE_NAME, "");
        if (columns != null && columns.size() > 0) {
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
        OssOutputFormatBuilder builder = new OssOutputFormatBuilder(fileType);
        builder.setPath(dealWithPath(path));
        builder.setEndpoint(endpoint);
        builder.setAccessKey(accessKey);
        builder.setSecretKey(secretKey);
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
        builder.setEnableDictionary(enableDictionary);

        return createOutput(dataSet, builder.finish());
    }

    private String dealWithPath(String path) {
        String pathWithPrefix = path;
        if (!StrUtil.startsWith(path, "s3a://")) {
            if (StrUtil.startsWith(path, "//")) {
                pathWithPrefix = "s3a:" + path;
            } else if (StrUtil.startsWith(path, "/")) {
                pathWithPrefix = "s3a:/" + path;
            } else {
                pathWithPrefix = "s3a://" + path;
            }
        }

        if (!StrUtil.endsWith(pathWithPrefix,"/")) {
            pathWithPrefix = pathWithPrefix + "/";
        }

        LOG.debug("Path = " + pathWithPrefix);
        return pathWithPrefix;
    }
}
