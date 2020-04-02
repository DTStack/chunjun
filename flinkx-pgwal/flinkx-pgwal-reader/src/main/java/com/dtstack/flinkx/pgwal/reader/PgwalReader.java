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

package com.dtstack.flinkx.pgwal.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.pgwal.PgWalConfigKeys;
import com.dtstack.flinkx.pgwal.format.PgWalInputFormatBuilder;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Date: 2019/12/13
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PgwalReader extends BaseDataReader {
    private String username;
    private String password;
    private String url;
    private String databaseName;
    private String cat;
    private boolean pavingData;
    private List<String> tableList;
    private Integer statusInterval;
    private Long lsn;
    private String slotName;
    private boolean allowCreateSlot;
    private boolean temporary;

    @SuppressWarnings("unchecked")
    public PgwalReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        username = readerConfig.getParameter().getStringVal(PgWalConfigKeys.KEY_USER_NAME);
        password = readerConfig.getParameter().getStringVal(PgWalConfigKeys.KEY_PASSWORD);
        url = readerConfig.getParameter().getStringVal(PgWalConfigKeys.KEY_URL);
        databaseName = readerConfig.getParameter().getStringVal(PgWalConfigKeys.KEY_DATABASE_NAME);
        cat = readerConfig.getParameter().getStringVal(PgWalConfigKeys.KEY_CATALOG);
        pavingData = readerConfig.getParameter().getBooleanVal(PgWalConfigKeys.KEY_PAVING_DATA, false);
        tableList = (List<String>) readerConfig.getParameter().getVal(PgWalConfigKeys.KEY_TABLE_LIST);
        statusInterval = readerConfig.getParameter().getIntVal(PgWalConfigKeys.KEY_STATUS_INTERVAL, 20000);
        lsn = readerConfig.getParameter().getLongVal(PgWalConfigKeys.KEY_LSN, 0);
        slotName = readerConfig.getParameter().getStringVal(PgWalConfigKeys.KEY_SLOT_NAME);
        allowCreateSlot = readerConfig.getParameter().getBooleanVal(PgWalConfigKeys.KEY_ALLOW_CREATE_SLOT, true);
        temporary = readerConfig.getParameter().getBooleanVal(PgWalConfigKeys.KEY_TEMPORARY, true);
    }

    @Override
    public DataStream<Row> readData() {
        PgWalInputFormatBuilder builder = new PgWalInputFormatBuilder();
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setUrl(url);
        builder.setDatabaseName(databaseName);
        builder.setCat(cat);
        builder.setPavingData(pavingData);
        builder.setTableList(tableList);
        builder.setRestoreConfig(restoreConfig);
        builder.setStatusInterval(statusInterval);
        builder.setLsn(lsn);
        builder.setSlotName(slotName);
        builder.setAllowCreateSlot(allowCreateSlot);
        builder.setTemporary(temporary);
        return createInput(builder.finish(), "pgwalreader");
    }
}
