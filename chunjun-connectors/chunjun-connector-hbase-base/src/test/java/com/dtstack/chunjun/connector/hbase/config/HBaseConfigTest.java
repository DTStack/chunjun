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

package com.dtstack.chunjun.connector.hbase.config;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class HBaseConfigTest {

    @Test
    public void testGetHBaseConfig() {
        Map<String, Object> confMap = Maps.newHashMap();
        List<FieldConfig> confList = Lists.newArrayList();
        HBaseConfig conf = new HBaseConfig();

        FieldConfig id = new FieldConfig();
        id.setName("stu.id");
        id.setType(TypeConfig.fromString("int"));

        FieldConfig address = new FieldConfig();
        address.setName("msg.address");
        address.setType(TypeConfig.fromString("string"));

        confList.add(id);
        confList.add(address);

        conf.setHbaseConfig(confMap);
        conf.setColumnMetaInfos(confList);
        conf.setEncoding(StandardCharsets.UTF_8.name());
        conf.setStartRowkey("start");
        conf.setEndRowkey("end");
        conf.setBinaryRowkey(true);
        conf.setTable("test_table");
        conf.setScanCacheSize(1000);

        conf.setNullMode("TEST_NULL_MODE");
        conf.setNullStringLiteral("N/A");
        conf.setWalFlag(true);
        conf.setWriteBufferSize(1000);
        conf.setRowkeyExpress("rowKey");
        conf.setVersionColumnIndex(1);
        conf.setVersionColumnValue("VERSION");

        Assert.assertEquals(confMap, conf.getHbaseConfig());
        Assert.assertEquals(confList, conf.getColumnMetaInfos());
        Assert.assertEquals(StandardCharsets.UTF_8.name(), conf.getEncoding());
        Assert.assertEquals("start", conf.getStartRowkey());
        Assert.assertEquals("end", conf.getEndRowkey());
        Assert.assertTrue(conf.isBinaryRowkey());
        Assert.assertEquals("test_table", conf.getTable());
        Assert.assertEquals(1000, conf.getScanCacheSize());

        Assert.assertEquals("TEST_NULL_MODE", conf.getNullMode());
        Assert.assertEquals("N/A", conf.getNullStringLiteral());
        Assert.assertTrue(conf.getWalFlag());
        Assert.assertEquals(1000, conf.getWriteBufferSize());
        Assert.assertEquals("rowKey", conf.getRowkeyExpress());
        Assert.assertEquals(new Integer(1), conf.getVersionColumnIndex());
        Assert.assertEquals("VERSION", conf.getVersionColumnValue());
    }
}
