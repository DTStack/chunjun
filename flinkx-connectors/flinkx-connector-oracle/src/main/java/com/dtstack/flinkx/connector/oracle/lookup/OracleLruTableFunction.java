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

package com.dtstack.flinkx.connector.oracle.lookup;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcLookupConf;
import com.dtstack.flinkx.connector.jdbc.lookup.JdbcLruTableFunction;
import com.dtstack.flinkx.enums.ECacheContentType;
import com.dtstack.flinkx.factory.DTThreadFactory;
import com.dtstack.flinkx.lookup.AbstractLruTableFunction;
import com.dtstack.flinkx.lookup.cache.CacheMissVal;
import com.dtstack.flinkx.lookup.cache.CacheObj;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.throwable.NoRestartException;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.ThreadUtil;
import com.google.common.collect.Lists;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.dtstack.flinkx.connector.jdbc.options.JdbcLookupOptions.*;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleLruTableFunction extends JdbcLruTableFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OracleLruTableFunction.class);


    public OracleLruTableFunction(
            JdbcConf jdbcConf,
            JdbcDialect jdbcDialect,
            LookupConf lookupConf,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        super(jdbcConf, jdbcDialect, lookupConf, fieldNames, keyNames, rowType);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", "true");
        super.open(context);
    }


}
