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

package com.dtstack.chunjun.connector.socket.table;

import com.dtstack.chunjun.connector.socket.entity.SocketConfig;
import com.dtstack.chunjun.connector.socket.options.SocketOptions;
import com.dtstack.chunjun.connector.socket.source.SocketDynamicTableSource;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.HashSet;
import java.util.Set;

public class SocketDynamicTableFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "socket-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SocketOptions.ADDRESS);
        options.add(SocketOptions.PARSE);
        options.add(SocketOptions.ENCODING);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Configuration options = new Configuration();
        context.getCatalogTable().getOptions().forEach(options::setString);
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();

        SocketConfig socketConfig = new SocketConfig();
        socketConfig.setAddress(options.get(SocketOptions.ADDRESS));
        socketConfig.setParse(options.get(SocketOptions.PARSE));
        socketConfig.setEncoding(options.get(SocketOptions.ENCODING));

        return new SocketDynamicTableSource(schema, socketConfig, context.getPhysicalRowDataType());
    }
}
