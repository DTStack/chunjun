/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadatahbase.inputformat;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hbase.HConstants;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MetadatahbaseInputFormatTest {

    protected MetadatahbaseInputFormat inputFormat = new MetadatahbaseInputFormat();

    private static TestingServer server;

    @Before
    public void createZkServer() throws Exception {
        server = new TestingServer(2191, true);
        server.start();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("localhost:2191")
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        client.start();
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hbase/table/test1", "init".getBytes());
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hbase/table/test2", "init".getBytes());
        client.close();
    }

    @Test
    public void testSetPath(){
        inputFormat.setPath("/hbase");
        Assert.assertEquals(inputFormat.path, "/hbase/table");
    }


    @Test
    public void testQueryCreateTimeMap(){
        Map<String, Object> hadoopConfig = new HashMap<>();
        hadoopConfig.put(HConstants.ZOOKEEPER_QUORUM, "localhost:2191");
        inputFormat.setPath("/hbase");
        Assert.assertEquals(inputFormat.queryCreateTimeMap(hadoopConfig).size(),0);
    }

    @After
    public void closedZkServer() throws IOException {
        server.close();
    }
}
