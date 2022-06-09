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

package com.dtstack.chunjun.connector.inceptor.source;

import com.dtstack.chunjun.connector.inceptor.conf.InceptorConf;
import com.dtstack.chunjun.connector.inceptor.util.InceptorDbUtil;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;

import org.apache.flink.core.io.InputSplit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.type.HiveDate;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;

/** @author liuliu 2022/2/24 */
public class InceptorSearchInputFormat extends JdbcInputFormat {

    @Override
    public void openInternal(InputSplit inputSplit) {
        super.openInternal(inputSplit);
    }

    @Override
    protected Connection getConnection() {
        return InceptorDbUtil.getConnection(
                (InceptorConf) jdbcConf, getRuntimeContext().getDistributedCache(), jobId);
    }

    @Override
    protected void queryForPolling(String startLocation) throws SQLException {
        // 每隔五分钟打印一次，(当前时间 - 任务开始时间) % 300秒 <= 一个间隔轮询周期
        if ((System.currentTimeMillis() - startTime) % 300000 <= jdbcConf.getPollingInterval()) {
            LOG.info("polling startLocation = {}", startLocation);
        } else {
            LOG.debug("polling startLocation = {}", startLocation);
        }

        boolean isNumber = StringUtils.isNumeric(startLocation);
        switch (type) {
            case TIMESTAMP:
                Timestamp ts =
                        isNumber
                                ? new Timestamp(Long.parseLong(startLocation))
                                : Timestamp.valueOf(startLocation);
                ps.setTimestamp(1, ts);
                break;
            case DATE:
                Date date =
                        isNumber
                                ? new HiveDate(Long.parseLong(startLocation))
                                : HiveDate.valueOf(startLocation);
                ps.setDate(1, date);
                break;
            default:
                if (isNumber) {
                    ps.setLong(1, Long.parseLong(startLocation));
                } else {
                    ps.setString(1, startLocation);
                }
        }
        resultSet = ps.executeQuery();
        hasNext = resultSet.next();
    }
}
