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

package com.dtstack.flinkx.connector.inceptor.sink;

import com.dtstack.flinkx.connector.inceptor.conf.InceptorConf;
import com.dtstack.flinkx.connector.inceptor.util.InceptorDbUtil;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.enums.Semantic;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.PluginUtil;

import org.apache.flink.api.common.cache.DistributedCache;

import java.sql.Connection;
import java.sql.SQLException;

/** @author liuliu 2022/2/24 */
public class InceptorSearchOutputFormat extends JdbcOutputFormat {

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        try {
            dbConn = getConnection();
            // 默认关闭事务自动提交，手动控制事务
            if (Semantic.EXACTLY_ONCE == semantic) {
                dbConn.setAutoCommit(false);
            }
            initColumnList();
            if (!EWriteMode.INSERT.name().equalsIgnoreCase(jdbcConf.getMode())) {
                throw new FlinkxRuntimeException(
                        String.format("inceptor search not support %s mode", jdbcConf.getMode()));
            }

            buildStmtProxy();
            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } finally {
            JdbcUtil.commit(dbConn);
        }
    }

    @Override
    public Connection getConnection() {
        DistributedCache distributedCache;
        try {
            distributedCache = getRuntimeContext().getDistributedCache();
        } catch (Exception e) {
            distributedCache = PluginUtil.createDistributedCacheFromContextClassLoader();
        }
        return InceptorDbUtil.getConnection((InceptorConf) jdbcConf, distributedCache, jobId);
    }
}
