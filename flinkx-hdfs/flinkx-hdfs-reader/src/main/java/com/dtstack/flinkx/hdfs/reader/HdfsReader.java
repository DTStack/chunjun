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


package com.dtstack.flinkx.hdfs.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.hdfs.HdfsConfigKeys;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/**
 * The reader plugin of Hdfs
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsReader extends BaseDataReader {
    protected String defaultFs;
    protected String fileType;
    protected String path;
    protected String fieldDelimiter;
    protected List<MetaColumn> metaColumns;
    protected Map<String, Object> hadoopConfig;
    protected String filterRegex;

    public HdfsReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        defaultFs = readerConfig.getParameter().getStringVal(HdfsConfigKeys.KEY_DEFAULT_FS);

        String fileName = readerConfig.getParameter().getStringVal(HdfsConfigKeys.KEY_FILE_NAME);
        if(StringUtils.isNotBlank(fileName)){
            //兼容平台逻辑
            path = readerConfig.getParameter().getStringVal(HdfsConfigKeys.KEY_PATH) + ConstantValue.SINGLE_SLASH_SYMBOL + fileName;
        }else{
            path = readerConfig.getParameter().getStringVal(HdfsConfigKeys.KEY_PATH);
        }

        fileType = readerConfig.getParameter().getStringVal(HdfsConfigKeys.KEY_FILE_TYPE);
        hadoopConfig = (Map<String, Object>) readerConfig.getParameter().getVal(HdfsConfigKeys.KEY_HADOOP_CONFIG);
        filterRegex = readerConfig.getParameter().getStringVal(HdfsConfigKeys.KEY_FILTER, "");

        fieldDelimiter = readerConfig.getParameter().getStringVal(HdfsConfigKeys.KEY_FIELD_DELIMITER);

        if(fieldDelimiter == null || fieldDelimiter.length() == 0) {
            fieldDelimiter = "\001";
        } else {
            fieldDelimiter = StringUtil.convertRegularExpr(fieldDelimiter);
        }

        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn(), false);
    }

    @Override
    public DataStream<Row> readData() {
        HdfsInputFormatBuilder builder = new HdfsInputFormatBuilder(fileType);
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setInputPaths(path);
        builder.setMetaColumn(metaColumns);
        builder.setHadoopConfig(hadoopConfig);
        builder.setFilterRegex(filterRegex);
        builder.setDefaultFs(defaultFs);
        builder.setDelimiter(fieldDelimiter);
        builder.setBytes(bytes);
        builder.setMonitorUrls(monitorUrls);
        builder.setRestoreConfig(restoreConfig);
        builder.setTestConfig(testConfig);
        builder.setLogConfig(logConfig);

        return createInput(builder.finish());
    }

}
