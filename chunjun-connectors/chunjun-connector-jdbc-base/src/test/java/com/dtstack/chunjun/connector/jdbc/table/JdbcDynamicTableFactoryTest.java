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

package com.dtstack.chunjun.connector.jdbc.table;

import com.dtstack.chunjun.connector.jdbc.converter.JdbcRawTypeConverterTest;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcDynamicTableSink;
import com.dtstack.chunjun.connector.jdbc.source.JdbcDynamicTableSource;
import com.dtstack.chunjun.converter.RawTypeConverter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/** @author liuliu 2022/8/19 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
    DynamicTableFactory.Context.class,
    LookupTableSource.LookupContext.class,
    CatalogTable.class,
    Configuration.class,
    ObjectIdentifier.class
})
public class JdbcDynamicTableFactoryTest {

    private static DynamicTableFactory.Context context;
    private static CatalogTable catalogTable;
    private static Configuration options;

    @BeforeClass
    public static void setup() {
        context = mock(DynamicTableFactory.Context.class);
        catalogTable = mock(CatalogTable.class);
        ObjectIdentifier objectIdentifier = mock(ObjectIdentifier.class);

        when(context.getCatalogTable().getOrigin()).thenReturn(catalogTable);
        when(context.getObjectIdentifier()).thenReturn(objectIdentifier);
        when(catalogTable.getOptions()).thenReturn(getOptions());
        when(catalogTable.getSchema()).thenReturn(getTableSchema());
        when(objectIdentifier.getObjectName()).thenReturn("test");
    }

    @Test
    public void initTest() {
        TestJdbcDynamicTableFactory testJdbcDynamicTableFactory = new TestJdbcDynamicTableFactory();

        // scan
        JdbcDynamicTableSource dynamicTableSource =
                (JdbcDynamicTableSource)
                        testJdbcDynamicTableFactory.createDynamicTableSource(context);
        dynamicTableSource.getScanRuntimeProvider(null);

        // lookup
        LookupTableSource.LookupContext lookupContext = mock(LookupTableSource.LookupContext.class);
        when(lookupContext.getKeys()).thenReturn(new int[][] {{0}, {0}});
        dynamicTableSource.getLookupRuntimeProvider(lookupContext);

        // sink
        JdbcDynamicTableSink dynamicTableSink =
                (JdbcDynamicTableSink) testJdbcDynamicTableFactory.createDynamicTableSink(context);
        dynamicTableSink.getSinkRuntimeProvider(null);
    }

    static class TestJdbcDynamicTableFactory extends JdbcDynamicTableFactory {

        @Override
        public String factoryIdentifier() {
            return "test-x";
        }

        @Override
        protected JdbcDialect getDialect() {
            return new JdbcDialect() {
                @Override
                public String dialectName() {
                    return "text-x";
                }

                @Override
                public boolean canHandle(String url) {
                    return true;
                }

                @Override
                public RawTypeConverter getRawTypeConverter() {
                    return JdbcRawTypeConverterTest::apply;
                }
            };
        }
    }

    public static Map<String, String> getOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://localhost:3306");
        options.put("username", "username");
        options.put("password", "password");
        options.put("schema", "schema");
        options.put("table-name", "table");
        options.put("scan.increment.column", "id");
        options.put("scan.increment.column-type", "int");
        options.put("scan.restore.columnname", "id");
        options.put("scan.restore.columntype", "int");
        return options;
    }

    public static TableSchema getTableSchema() {
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.field("id", DataTypes.INT());
        builder.field("name", DataTypes.STRING());
        return builder.build();
    }
}
