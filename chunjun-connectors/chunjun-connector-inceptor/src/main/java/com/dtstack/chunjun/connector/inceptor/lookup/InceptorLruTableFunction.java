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
package com.dtstack.chunjun.connector.inceptor.lookup;

import com.dtstack.chunjun.connector.inceptor.conf.InceptorConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.lookup.JdbcLruTableFunction;
import com.dtstack.chunjun.lookup.conf.LookupConf;
import com.dtstack.chunjun.security.KerberosUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLClient;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author dujie @Description
 * @createTime 2022-01-20 04:50:00
 */
public class InceptorLruTableFunction extends JdbcLruTableFunction {
    private final InceptorConf inceptorConf;
    private UserGroupInformation ugi;

    public InceptorLruTableFunction(
            InceptorConf inceptorConf,
            JdbcDialect jdbcDialect,
            LookupConf lookupConf,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        super(inceptorConf, jdbcDialect, lookupConf, fieldNames, keyNames, rowType);
        this.inceptorConf = inceptorConf;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.ugi = KerberosUtil.loginAndReturnUgi(inceptorConf.getHadoopConfig());
    }

    @Override
    protected void asyncQueryData(
            CompletableFuture<Collection<RowData>> future,
            SQLClient rdbSqlClient,
            AtomicLong failCounter,
            AtomicBoolean finishFlag,
            CountDownLatch latch,
            Object... keys) {
        ugi.doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        doAsyncQueryData(
                                future, rdbSqlClient, failCounter, finishFlag, latch, keys);
                        return null;
                    }
                });
    }

    /**
     * get jdbc connection
     *
     * @return
     */
    @Override
    public JsonObject createJdbcConfig(Map<String, Object> druidConfMap) {
        JsonObject jdbcConfig = super.createJdbcConfig(druidConfMap);
        JsonObject clientConfig = new JsonObject();

        Iterator<Map.Entry<String, Object>> iterator = jdbcConfig.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();

            if (Objects.nonNull(next.getValue())) {
                clientConfig.put(next.getKey(), next.getValue());
            }
        }
        return clientConfig;
    }
}
