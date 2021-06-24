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


package com.dtstack.flinkx.hbase2.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.util.ValueUtil;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.hbase2.HbaseConfigConstants.DEFAULT_WAL_FLAG;
import static com.dtstack.flinkx.hbase2.HbaseConfigConstants.DEFAULT_WRITE_BUFFER_SIZE;
import static com.dtstack.flinkx.hbase2.HbaseConfigKeys.*;

/**
 * The Writer plugin of HBase
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class Hbase2Writer extends BaseDataWriter {

    private String tableName;
    private Map<String,Object> hbaseConfig;
    private String encoding;
    private String nullMode;
    private Boolean walFlag;
    private long writeBufferSize;

    private List<String> columnTypes;
    private List<String> columnNames;
    private String rowkeyExpress;

    private Integer versionColumnIndex;
    private String versionColumnValue;

    public Hbase2Writer(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();

        tableName = writerConfig.getParameter().getStringVal(KEY_TABLE);
        hbaseConfig = (Map<String, Object>) writerConfig.getParameter().getVal(KEY_HBASE_CONFIG);
        encoding = writerConfig.getParameter().getStringVal(KEY_ENCODING);
        nullMode = writerConfig.getParameter().getStringVal(KEY_NULL_MODE);
        walFlag = writerConfig.getParameter().getBooleanVal(KEY_WAL_FLAG, DEFAULT_WAL_FLAG);
        writeBufferSize = writerConfig.getParameter().getLongVal(KEY_WRITE_BUFFER_SIZE, DEFAULT_WRITE_BUFFER_SIZE);

        List columns = writerConfig.getParameter().getColumn();
        if(CollectionUtils.isNotEmpty(columns)) {
            columnTypes = new ArrayList<>();
            columnNames = new ArrayList<>();
            for(int i = 0; i < columns.size(); ++i) {
                Map sm = (Map) columns.get(i);
                columnNames.add((String) sm.get(KEY_COLUMN_NAME));
                columnTypes.add((String) sm.get(KEY_COLUMN_TYPE));
            }
        }

        Object rowKeyInfo = writerConfig.getParameter().getStringVal(KEY_ROW_KEY_COLUMN);
        rowkeyExpress = buildRowKeyExpress(rowKeyInfo);

        Map<String,Object> versionColumn = (Map<String, Object>) writerConfig.getParameter().getVal(KEY_VERSION_COLUMN);
        if(versionColumn != null) {
            versionColumnIndex = (Integer) versionColumn.get(KEY_VERSION_COLUMN_INDEX);
            versionColumnValue = (String) versionColumn.get(KEY_VERSION_COLUMN_VALUE);
        }
    }

    /**
     * Compatible with old formats
     */
    private String buildRowKeyExpress(Object rowKeyInfo){
        if (rowKeyInfo == null){
            return null;
        }

        if(rowKeyInfo instanceof String){
            return rowKeyInfo.toString();
        }

        if(!(rowKeyInfo instanceof List)){
            return null;
        }

        StringBuilder expressBuilder = new StringBuilder();

        for (Map item : ((List<Map>) rowKeyInfo)) {
            Integer index = ValueUtil.getInt(item.get(KEY_ROW_KEY_COLUMN_INDEX));
            if (index != null && index != -1) {
                expressBuilder.append(String.format("$(%s)", columnNames.get(index)));
                continue;
            }

            String value = (String) item.get(KEY_ROW_KEY_COLUMN_VALUE);
            if (StringUtils.isNotEmpty(value)) {
                expressBuilder.append(value);
            }
        }

        return expressBuilder.toString();
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        HbaseOutputFormatBuilder builder = new HbaseOutputFormatBuilder();
        builder.setHbaseConfig(hbaseConfig);
        builder.setTableName(tableName);
        builder.setEncoding(encoding);
        builder.setNullMode(nullMode);
        builder.setWalFlag(walFlag);
        builder.setWriteBufferSize(writeBufferSize);
        builder.setColumnNames(columnNames);
        builder.setColumnTypes(columnTypes);
        builder.setRowkeyExpress(rowkeyExpress);
        builder.setVersionColumnIndex(versionColumnIndex);
        builder.setVersionColumnValues(versionColumnValue);
        builder.setMonitorUrls(monitorUrls);
        builder.setErrorRatio(errorRatio);
        builder.setErrors(errors);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);

        return createOutput(dataSet, builder.finish());
    }
}
