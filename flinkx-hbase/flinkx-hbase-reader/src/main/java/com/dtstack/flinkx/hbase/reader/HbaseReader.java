/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.hbase.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.hbase.HbaseConfigConstants;
import com.dtstack.flinkx.hbase.HbaseConfigKeys;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The reader plugin of Hbase
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HbaseReader extends BaseDataReader {

    private static Logger LOG = LoggerFactory.getLogger(HbaseReader.class);

    protected List<String> columnName;
    protected List<String> columnType;
    protected List<String> columnValue;
    protected List<String> columnFormat;
    protected String encoding;
    protected Map<String,Object> hbaseConfig;
    protected String startRowkey;
    protected String endRowkey;
    protected boolean isBinaryRowkey;
    protected String tableName;
    protected int scanCacheSize;

    public HbaseReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        tableName = readerConfig.getParameter().getStringVal(HbaseConfigKeys.KEY_TABLE);
        hbaseConfig = (Map<String, Object>) readerConfig.getParameter().getVal(HbaseConfigKeys.KEY_HBASE_CONFIG);

        Map range = (Map) readerConfig.getParameter().getVal(HbaseConfigKeys.KEY_RANGE);
        if(range != null) {
            startRowkey = (String) range.get(HbaseConfigKeys.KEY_START_ROW_KEY);
            endRowkey = (String) range.get(HbaseConfigKeys.KEY_END_ROW_KEY);
            isBinaryRowkey = (Boolean) range.get(HbaseConfigKeys.KEY_IS_BINARY_ROW_KEY);
        }

        encoding = readerConfig.getParameter().getStringVal(HbaseConfigKeys.KEY_ENCODING);
        scanCacheSize = readerConfig.getParameter().getIntVal(HbaseConfigKeys.KEY_SCAN_CACHE_SIZE, HbaseConfigConstants.DEFAULT_SCAN_CACHE_SIZE);

        List columns = readerConfig.getParameter().getColumn();
        if(columns != null && columns.size() > 0) {
            columnName = new ArrayList<>();
            columnType = new ArrayList<>();
            columnValue = new ArrayList<>();
            columnFormat = new ArrayList<>();
            for(int i = 0; i < columns.size(); ++i) {
                Map sm = (Map) columns.get(i);
                columnName.add((String) sm.get("name"));
                columnType.add((String) sm.get("type"));
                columnValue.add((String) sm.get("value"));
                columnFormat.add((String) sm.get("format"));
            }

            LOG.info("init column finished");
        } else{
            throw new IllegalArgumentException("column argument error");
        }
    }

    @Override
    public DataStream<Row> readData() {
        HbaseInputFormatBuilder builder = new HbaseInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setColumnFormats(columnFormat);
        builder.setColumnNames(columnName);
        builder.setColumnTypes(columnType);
        builder.setColumnValues(columnValue);
        builder.setEncoding(encoding);
        builder.setEndRowkey(endRowkey);
        builder.setHbaseConfig(hbaseConfig);
        builder.setStartRowkey(startRowkey);
        builder.setIsBinaryRowkey(isBinaryRowkey);
        builder.setTableName(tableName);
        builder.setBytes(bytes);
        builder.setMonitorUrls(monitorUrls);
        builder.setScanCacheSize(scanCacheSize);
        builder.setMonitorUrls(monitorUrls);
        builder.setTestConfig(testConfig);
        builder.setLogConfig(logConfig);

        return createInput(builder.finish());
    }

}
