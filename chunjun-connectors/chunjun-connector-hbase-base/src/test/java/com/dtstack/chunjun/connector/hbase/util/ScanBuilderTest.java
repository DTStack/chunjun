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

package com.dtstack.chunjun.connector.hbase.util;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;

import org.apache.flink.table.api.DataTypes;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ScanBuilderTest {

    private final HBaseTableSchema tableSchema = new HBaseTableSchema();

    private final List<FieldConf> confList = Lists.newArrayList();

    @Before
    public void setUp() {
        tableSchema.addColumn("stu", "id", DataTypes.INT());
        tableSchema.addColumn("msg", "address", DataTypes.STRING());

        FieldConf id = new FieldConf();
        id.setName("stu.id");
        id.setType("int");

        FieldConf address = new FieldConf();
        address.setName("msg.address");
        address.setType("string");

        confList.add(id);
        confList.add(address);
    }

    @Test
    public void testBuilder() {
        ScanBuilder forSql = ScanBuilder.forSql(tableSchema);
        ScanBuilder forSync = ScanBuilder.forSync(confList);

        byte[][] forSqlFamilies = forSql.buildScan().getFamilies();
        byte[][] forSyncFamilies = forSync.buildScan().getFamilies();

        List<String> forSqlFamiliesResult = Lists.newArrayList();
        for (byte[] forSqlFamily : forSqlFamilies) {
            forSqlFamiliesResult.add(new String(forSqlFamily));
        }
        List<String> forSyncFamiliesResult = Lists.newArrayList();
        for (byte[] forSyncFamily : forSyncFamilies) {
            forSyncFamiliesResult.add(new String(forSyncFamily));
        }

        Assert.assertTrue(forSqlFamiliesResult.contains("stu"));
        Assert.assertTrue(forSyncFamiliesResult.contains("msg"));
    }
}
