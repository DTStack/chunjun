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

package com.dtstack.flinkx.solr.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.solr.SolrConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The Reader plugin of Solr
 *
 * Company: www.dtstack.com
 * @author shifang
 */
public class SolrReader extends BaseDataReader {

    private static Logger LOG = LoggerFactory.getLogger(SolrReader.class);

    private String address;

    protected List<String> filters;

    protected List<MetaColumn> metaColumns;

    protected int batchSize;

    public SolrReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        address = readerConfig.getParameter().getStringVal(SolrConstant.KEY_ADDRESS);
        batchSize = readerConfig.getParameter().getIntVal(SolrConstant.KEY_BATCH_SIZE, 10);
        filters = (List<String>)readerConfig.getParameter().getVal(SolrConstant.KEY_FILTERS);
        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn(), false);
    }

    @Override
    public DataStream<Row> readData() {
        SolrInputFormatBuilder builder = new SolrInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setColumns(metaColumns);
        builder.setFilters(filters);
        builder.setAddress(address);
        builder.setBatchSize(batchSize);
        List<String> columnNames = new ArrayList<>();
        for (MetaColumn metaColumn : metaColumns) {
            if (StringUtils.isBlank(metaColumn.getValue())) {
                columnNames.add(metaColumn.getName());
            }
        }
        builder.setColumnNames(columnNames);
        return createInput(builder.finish());
    }

}
