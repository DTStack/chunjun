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

package com.dtstack.flinkx.odps.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.DataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static com.dtstack.flinkx.odps.OdpsConfigKeys.*;

/**
 * The reader plugin of Odps
 * @author huyifan.zju@163.com
 * @date 2018-1-17
 */
public class OdpsReader extends DataReader {
    private Map<String,String> odpsConfig;
    protected List<String> columnName;
    protected List<String> columnType;
    protected List<String> columnValue;

    protected String tableName;
    protected String partition;


    public OdpsReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        odpsConfig = (Map<String, String>) readerConfig.getParameter().getVal(KEY_ODPS_CONFIG);
        tableName = readerConfig.getParameter().getStringVal(KEY_TABLE);
        partition = readerConfig.getParameter().getStringVal(KEY_PARTITION);

        List columns = readerConfig.getParameter().getColumn();
        if(columns != null && columns.size() > 0) {
            if(columns.get(0) instanceof Map) {
                columnType = new ArrayList<>();
                columnValue = new ArrayList<>();
                columnName = new ArrayList<>();
                for(int i = 0; i < columns.size(); ++i) {
                    Map sm = (Map) columns.get(i);
                    columnType.add((String) sm.get(KEY_COLUMN_TYPE));
                    columnValue.add((String) sm.get(KEY_COLUMN_VALUE));
                    columnName.add((String) sm.get(KEY_COLUMN_NAME));
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
        OdpsInputFormatBuilder builder = new OdpsInputFormatBuilder();

        builder.setColumnName(columnName);
        builder.setColumnType(columnType);
        builder.setColumnValue(columnValue);
        builder.setOdpsConfig(odpsConfig);
        builder.setTableName(tableName);
        builder.setPartition(partition);
        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);

        return createInput(builder.finish(), "odpsreader");
    }
}
