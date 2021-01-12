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

package com.dtstack.flinkx.phoenix5.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.phoenix5.Phoenix5ConfigKeys;
import com.dtstack.flinkx.phoenix5.Phoenix5DatabaseMeta;
import com.dtstack.flinkx.phoenix5.format.Phoenix5InputFormat;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormatBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.HConstants;

/**
 * phoenix reader plugin
 *
 * Company: www.dtstack.com
 * @author wuhui
 */
public class Phoenix5Reader extends JdbcDataReader {
    private boolean readFromHbase;
    private int scanCacheSize;
    private int scanBatchSize;

    public Phoenix5Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        readFromHbase = readerConfig.getParameter().getBooleanVal(Phoenix5ConfigKeys.KEY_READ_FROM_HBASE, false);
        scanCacheSize = readerConfig.getParameter().getIntVal(Phoenix5ConfigKeys.KEY_SCAN_CACHE_SIZE, HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
        scanBatchSize = readerConfig.getParameter().getIntVal(Phoenix5ConfigKeys.KEY_SCAN_BATCH_SIZE, -1);
        setDatabaseInterface(new Phoenix5DatabaseMeta());
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        Phoenix5InputFormatBuilder builder = new Phoenix5InputFormatBuilder(new Phoenix5InputFormat());
        builder.setReadFromHbase(readFromHbase);
        builder.setScanCacheSize(scanCacheSize);
        builder.setScanBatchSize(scanBatchSize);
        builder.setWhere(where);
        builder.setCustomSql(customSql);
        builder.setOrderByColumn(orderByColumn);
        return builder;
    }

}

