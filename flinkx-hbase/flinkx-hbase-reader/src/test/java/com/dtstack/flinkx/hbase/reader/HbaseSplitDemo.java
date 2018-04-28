package com.dtstack.flinkx.hbase.reader;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import java.io.IOException;

/**
 * Created by softfly on 17/7/25.
 */
public class HbaseSplitDemo {

    private static void split() {

    }

    public static void main(String[] args) throws IOException {

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "172.16.1.151" );
        conf.set("zookeeper.znode.parent", "/hbase2");

        Connection conn = ConnectionFactory.createConnection(conf);
        //Table table = conn.getTable(TableName.valueOf("tb2"));

        RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf("tb2"));
        regionLocator.getStartEndKeys();

    }

}
