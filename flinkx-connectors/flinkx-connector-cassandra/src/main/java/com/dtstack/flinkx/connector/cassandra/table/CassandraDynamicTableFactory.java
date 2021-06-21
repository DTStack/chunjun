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

package com.dtstack.flinkx.connector.cassandra.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.dtstack.flinkx.connector.cassandra.conf.CassandraSinkConf;
import com.dtstack.flinkx.connector.cassandra.sink.CassandraDynamicTableSink;
import com.dtstack.flinkx.throwable.NoRestartException;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraCommonOptions.CLUSTER_NAME;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraCommonOptions.CONSISTENCY;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraCommonOptions.HOST;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraCommonOptions.HOST_DISTANCE;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraCommonOptions.KEY_SPACES;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraCommonOptions.PASSWORD;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraCommonOptions.PORT;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraCommonOptions.TABLE_NAME;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraCommonOptions.USER_NAME;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraSinkOptions.ASYNC_WRITE;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraSinkOptions.CONNECT_TIMEOUT_MILLISECONDS;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraSinkOptions.CORE_CONNECTIONS_PER_HOST;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraSinkOptions.MAX_CONNECTIONS__PER_HOST;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraSinkOptions.MAX_QUEUE_SIZE;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraSinkOptions.MAX_REQUESTS_PER_CONNECTION;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraSinkOptions.POOL_TIMEOUT_MILLISECONDS;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraSinkOptions.READ_TIME_OUT_MILLISECONDS;
import static com.dtstack.flinkx.connector.cassandra.optinos.CassandraSinkOptions.USE_SSL;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIED = "cassandra-x";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();
        validateConfigOptions(config);

        // 3.封装参数
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        CassandraSinkConf sinkConf = CassandraSinkConf.from(helper.getOptions());

        return new CassandraDynamicTableSink(sinkConf, physicalSchema);
    }

    private void validateConfigOptions(ReadableConfig config) {
        Optional<String> host = config.getOptional(HOST);
        Optional<String> tableName = config.getOptional(TABLE_NAME);
        StringBuilder stringBuilder = new StringBuilder(256);

        if (host.isPresent() && !host.get().isEmpty()) {
            stringBuilder.append("Cassandra host can not be empty.\n");
        }

        if (tableName.isPresent() && !tableName.get().isEmpty()) {
            stringBuilder.append("Cassandra table-name can not be empty.\n");
        }

        if (stringBuilder.length() > 0) {
            throw new NoRestartException(stringBuilder.toString());
        }
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return null;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIED;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredConfigOptions = new HashSet<>();
        requiredConfigOptions.add(HOST);
        requiredConfigOptions.add(TABLE_NAME);
        return requiredConfigOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();

        // common
        optionalOptions.add(PORT);
        optionalOptions.add(USER_NAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(KEY_SPACES);
        optionalOptions.add(HOST_DISTANCE);
        optionalOptions.add(CLUSTER_NAME);
        optionalOptions.add(CONSISTENCY);

        // sink
        optionalOptions.add(CORE_CONNECTIONS_PER_HOST);
        optionalOptions.add(MAX_CONNECTIONS__PER_HOST);
        optionalOptions.add(MAX_REQUESTS_PER_CONNECTION);
        optionalOptions.add(MAX_QUEUE_SIZE);
        optionalOptions.add(READ_TIME_OUT_MILLISECONDS);
        optionalOptions.add(CONNECT_TIMEOUT_MILLISECONDS);
        optionalOptions.add(POOL_TIMEOUT_MILLISECONDS);
        optionalOptions.add(USE_SSL);
        optionalOptions.add(ASYNC_WRITE);

        // TODO source、 lookup

        return optionalOptions;
    }
}
