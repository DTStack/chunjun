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
    public void testQuote(){
        Assert.assertEquals(inputFormat.quote("table"), "table");
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
