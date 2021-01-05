package com.dtstack.flinkx.metadataphoenix5.util;


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
        client = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000,3))
                .build();
        client.start();
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hbase/table/test1", "init".getBytes());
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hbase/table/test2", "init".getBytes());

        zooKeeper = ZkHelper.createZkClient("localhost:2181", ZkHelper.DEFAULT_TIMEOUT);
    }

    @Test
    public void testCreateSingleZkClient(){
        Assert.assertNotNull(zooKeeper);
    }

    @Test
    public void testGetChildren() {
        List<String> list = ZkHelper.getChildren(zooKeeper, "/hbase/table");
        Assert.assertNotNull(list);
        Assert.assertEquals(2, list.size());
    }

    @Test
    public void testGetCreateTime(){
        Assert.assertNotEquals(ZkHelper.getCreateTime(zooKeeper, "/hbase/table/test1"), 0L);
    }


    @After
    public void closeZkServer() throws IOException {
        ZkHelper.closeZooKeeper(zooKeeper);
        client.close();
        server.close();
    }

}
