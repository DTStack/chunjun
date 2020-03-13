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
package com.dtstack.flinkx.odps.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.odps.OdpsConfigKeys;
import com.dtstack.flinkx.odps.OdpsUtil;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import java.util.List;
import java.util.Map;

/**
 * The writer plugin of Odps
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class OdpsWriter extends BaseDataWriter {

    private Map<String,String> odpsConfig;

    protected String[] columnName;

    protected String[] columnType;

    protected String tableName;

    protected String partition;

    protected String projectName;

    protected long bufferSize;

    public OdpsWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        odpsConfig = (Map<String, String>) writerConfig.getParameter().getVal(OdpsConfigKeys.KEY_ODPS_CONFIG);
        tableName = writerConfig.getParameter().getStringVal(OdpsConfigKeys.KEY_TABLE);
        partition = writerConfig.getParameter().getStringVal(OdpsConfigKeys.KEY_PARTITION);
        mode = writerConfig.getParameter().getStringVal(OdpsConfigKeys.KEY_WRITE_MODE);
        projectName = writerConfig.getParameter().getStringVal(OdpsConfigKeys.KEY_PROJECT);

        bufferSize = writerConfig.getParameter().getLongVal(OdpsConfigKeys.KEY_BUFFER_SIZE, 0);
        if (bufferSize == 0){
            bufferSize = OdpsUtil.BUFFER_SIZE_DEFAULT;
        } else {
            bufferSize = bufferSize * 1024 * 1024;
        }

        List columns = (List) writerConfig.getParameter().getVal(OdpsConfigKeys.KEY_COLUMN_LIST);
        if(CollectionUtils.isNotEmpty(columns)) {
            columnName = new String[columns.size()];
            columnType = new String[columns.size()];
            for(int i = 0; i < columns.size(); ++i) {
                Map sm = (Map) columns.get(i);
                columnName[i] = (String) sm.get(OdpsConfigKeys.KEY_COLUMN_NAME);
                columnType[i] = (String) sm.get(OdpsConfigKeys.KEY_COLUMN_TYPE);
            }
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        OdpsOutputFormatBuilder builder = new OdpsOutputFormatBuilder();

        builder.setPartition(partition);
        builder.setColumnNames(columnName);
        builder.setColumnTypes(columnType);
        builder.setWriteMode(mode);
        builder.setTableName(tableName);
        builder.setOdpsConfig(odpsConfig);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setErrorRatio(errorRatio);
        builder.setErrors(errors);
        builder.setBufferSize(bufferSize);
        builder.setRestoreConfig(restoreConfig);

        return createOutput(dataSet, builder.finish());
    }
}
