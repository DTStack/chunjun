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
package com.dtstack.flinkx.clickhouse.reader;

import com.dtstack.flinkx.clickhouse.core.ClickhouseConfig;
import com.dtstack.flinkx.clickhouse.core.ClickhouseConfigBuilder;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.DataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.dtstack.flinkx.clickhouse.core.ClickhouseConfigKeys.*;
/**
 * Date: 2019/11/05
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class ClickhouseReader extends DataReader {
    private ClickhouseConfig clickhouseConfig;

    @SuppressWarnings("unchecked")
    public ClickhouseReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        ReaderConfig.ParameterConfig parameterConfig = readerConfig.getParameter();
        List<MetaColumn> column = MetaColumn.getMetaColumns(parameterConfig.getColumn());
        Map<String, Object> map = (Map<String, Object>)parameterConfig.getVal(KEY_CLICKHOUSE_PROP);
        Properties clickhouseProp = new Properties();
        if(map != null && map.size() > 0){
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                clickhouseProp.put(entry.getKey(), entry.getValue());
            }
        }

        clickhouseConfig = new ClickhouseConfigBuilder()
                .withUrl(parameterConfig.getStringVal(KEY_URL))
                .withUsername(parameterConfig.getStringVal(KEY_USERNAME))
                .withPassword(parameterConfig.getStringVal(KEY_PASSWORD))
                .withTable(parameterConfig.getStringVal(KEY_TABLE))
                .withColumn(column)
                .withClickhouseProp(clickhouseProp)
                .withQueryTimeOut(parameterConfig.getIntVal(KEY_QUERY_TIME_OUT, 0))
                .withSplitKey(parameterConfig.getStringVal(KEY_SPLITKEY))
                .withFilter(parameterConfig.getStringVal(KEY_FILTER))
                .withIncreColumn(parameterConfig.getStringVal(KEY_INCRE_COLUMN))
                .withStartLocation(parameterConfig.getStringVal(KEY_START_LOCATION, null))
                .withBatchInterval(parameterConfig.getIntVal(KEY_BATCH_INTERVAL, 10000))
                .withPreSql(parameterConfig.getStringVal(KEY_PRESQL))
                .withPostSql(parameterConfig.getStringVal(KEY_POSTSQL))
                .build();
    }


    @Override
    public DataStream<Row> readData() {
        ClickhouseInputFormatBuilder builder = new ClickhouseInputFormatBuilder();
        builder.setClickhouseConfig(clickhouseConfig);
        return createInput(builder.finish(), "clickhousereader");
    }
}
