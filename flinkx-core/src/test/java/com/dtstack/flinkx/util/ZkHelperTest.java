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

package com.dtstack.flinkx.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZkHelperTest {

    private static TestingServer server;

    private static CuratorFramework client;

    private static ZooKeeper zooKeeper;

    @Before
    public void createZkServer() throws Exception {
        server = new TestingServer(2181, true);
        server.start();
        client =
                CuratorFrameworkFactory.builder()
                        .connectString("localhost:2181")
                        .connectionTimeoutMs(5000)
                        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                        .build();
        client.start();
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath("/hbase/table/test1", "init".getBytes());
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath("/hbase/table/test2", "init".getBytes());

        zooKeeper = ZkHelper.createZkClient("localhost:2181", ZkHelper.DEFAULT_TIMEOUT);
    }

    @Test
    public void testCreateSingleZkClient() {
        Assert.assertNotNull(zooKeeper);
    }

    @Test
    public void testGetChildren() {
        List<String> list = ZkHelper.getChildren(zooKeeper, "/hbase/table");
        Assert.assertNotNull(list);
        Assert.assertEquals(2, list.size());
    }

    @Test
    public void testGetCreateTime() {
        Assert.assertNotEquals(ZkHelper.getCreateTime(zooKeeper, "/hbase/table/test1"), 0L);
    }

    @After
    public void closeZkServer() throws IOException {
        ZkHelper.closeZooKeeper(zooKeeper);
        client.close();
        server.close();
    }
}
