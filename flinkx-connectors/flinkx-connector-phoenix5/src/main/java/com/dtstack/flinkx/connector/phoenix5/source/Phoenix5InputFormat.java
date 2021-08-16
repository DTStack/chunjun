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

package com.dtstack.flinkx.connector.phoenix5.source;

import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.flinkx.connector.phoenix5.util.Phoenix5Util;

import org.apache.flink.core.io.InputSplit;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class Phoenix5InputFormat extends JdbcInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(Phoenix5InputFormat.class);

    // phoenix resolve table meta data by retrieving a row of data.
    @Override
    protected Pair<List<String>, List<String>> getTableMetaData() {
        LOG.info("Obtain meta data , table = {}.", jdbcConf.getTable());
        return Phoenix5Util.getTableMetaData(
                jdbcConf.getColumn(), jdbcConf.getTable(), getConnection());
    }

    @Override
    public void openInternal(InputSplit inputSplit) {
        super.openInternal(inputSplit);
        LOG.info(" Open phoenix5 input format internal success !");
    }

    /**
     * 获取数据库连接，用于子类覆盖
     *
     * @return connection
     */
    @SuppressWarnings("AlibabaRemoveCommentedCode")
    @Override
    protected Connection getConnection() {
        Connection conn =
                Phoenix5Util.getConnection(jdbcDialect.defaultDriverName().get(), jdbcConf);
        LOG.info("Obtain a phoenix5 connection success !");
        return conn;
    }
}
