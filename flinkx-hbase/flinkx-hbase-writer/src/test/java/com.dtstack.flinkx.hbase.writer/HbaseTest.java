package com.dtstack.flinkx.hbase.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/3/26
 */
public class HbaseTest {
    static Map<String,Object> conf2 = null;
    static Configuration conf = null;
    static HBaseAdmin admin = null;
    static Connection connection = null;
    static Table table = null;

    @Before
    public void init() throws IOException {
        // 创建配置类
        conf2 = new HashMap<>();
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.16.8.193");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.rootdir", "file:///data/hbase/hbase");
        conf.set("hbase.zookeeper.property.dataDir", "/data/hbase/zookeeper");
        conf2.put("hbase.zookeeper.quorum", "172.16.8.193");
        conf2.put("hbase.zookeeper.property.clientPort", "2181");
        conf2.put("hbase.rootdir", "file:///data/hbase/hbase");
        conf2.put("hbase.zookeeper.property.dataDir", "/data/hbase/zookeeper");

        System.out.println(HBaseUtil.checkConnection(conf2));

//        // 创建表管理类
//        admin = new HBaseAdmin(conf);
//        // 创建连接并获取表
//        connection = ConnectionFactory.createConnection(conf);
//        table = connection.getTable(TableName.valueOf("hbase_writer"));
    }

    @Test
    public void sss(){
        System.out.println(12);
    }

    @Test
    public void testCreateTable() throws IOException {
        // 创建表描述类
        HTableDescriptor tableDesc = new HTableDescriptor("hbase_writer2");
        // 创建列族描述符类
        HColumnDescriptor myCf1 = new HColumnDescriptor("cf1");
        // 创建表操作(一般用shell，很少用代码)
        tableDesc.addFamily(myCf1);
        admin.createTable(tableDesc);
    }

    @Test
    public void testDeleteTable() throws IOException {
        admin.disableTable("test");
        admin.deleteTable("test");
    }

    @Test
    public void testInsertSingleRecord() throws IOException {
        Put rk1 = new Put(Bytes.toBytes("rk1"));
        rk1.addColumn(Bytes.toBytes("my_cf1"), Bytes.toBytes("name"), Bytes.toBytes("NikoBelic"));
        table.put(rk1);
    }

    @Test
    public void testInsertMultipleRecords() throws IOException {
        List<Put> rowKeyList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Put rk = new Put(Bytes.toBytes(i));
            rk.addColumn(Bytes.toBytes("my_cf1"), Bytes.toBytes("name"), Bytes.toBytes("Name" + i));
            rk.addColumn(Bytes.toBytes("my_cf2"), Bytes.toBytes("age"), Bytes.toBytes("Age" + i));
            rowKeyList.add(rk);
        }
        table.put(rowKeyList);
    }

    @Test
    public void testDeleteRecord() throws IOException {
        Delete delete = new Delete(Bytes.toBytes(5));
        table.delete(delete);
    }

    @Test
    public void testGetSingleRecord() throws IOException {
        Get get = new Get(Bytes.toBytes(4));
        Result result = table.get(get);
        printRow(result);
    }

    @Test
    public void testGetMultipleRecords() throws IOException {
        Scan scan = new Scan();
        // 这里的Row设置的是RowKey！而不是行号
        scan.setStartRow(Bytes.toBytes(1));
        scan.setStopRow(Bytes.toBytes(10));
        //scan.setStartRow(Bytes.toBytes("Shit"));
        //scan.setStopRow(Bytes.toBytes("Shit2"));
        ResultScanner scanner = table.getScanner(scan);
        printResultScanner(scanner);
    }

    @Test
    public void testFilter() throws IOException {
        /*
        FilterList 代表一个过滤器列表，可以添加多个过滤器进行查询，多个过滤器之间的关系有：
        与关系（符合所有）：FilterList.Operator.MUST_PASS_ALL
        或关系（符合任一）：FilterList.Operator.MUST_PASS_ONE
         过滤器的种类：
            SingleColumnValueFilter - 列值过滤器
                - 过滤列值的相等、不等、范围等
                - 注意：如果过滤器过滤的列在数据表中有的行中不存在，那么这个过滤器对此行无法过滤。
            ColumnPrefixFilter - 列名前缀过滤器
                - 过滤指定前缀的列名
            MultipleColumnPrefixFilter - 多个列名前缀过滤器
                - 过滤多个指定前缀的列名
            RowFilter - rowKey过滤器
                - 通过正则，过滤rowKey值。
                - 通常根据rowkey来指定范围时，使用scan扫描器的StartRow和StopRow方法比较好。
         */
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Scan s1 = new Scan();
        // 1. 列值过滤器
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes("my_cf1"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("Name1"));
        // 2. 列名前缀过滤器
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("ad"));
        // 3. 多个列值前缀过滤器
        byte[][] prefixes = new byte[][]{Bytes.toBytes("na"), Bytes.toBytes("ad")};
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(prefixes);
        // 4. RowKey过滤器
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^Shit"));
        // 若设置，则只返回指定的cell，同一行中的其他cell不返回
        //s1.addColumn(Bytes.toBytes("my_cf1"), Bytes.toBytes("name"));
        //s1.addColumn(Bytes.toBytes("my_cf2"), Bytes.toBytes("age"));
        // 设置过滤器
        //filterList.addFilter(singleColumnValueFilter);
        //filterList.addFilter(columnPrefixFilter);
        //filterList.addFilter(multipleColumnPrefixFilter);
        filterList.addFilter(rowFilter);
        s1.setFilter(filterList);
        ResultScanner scanner = table.getScanner(s1);
        printResultScanner(scanner);
    }

    public void printResultScanner(ResultScanner scanner) {
        for (Result row : scanner) {
            // Hbase中一个RowKey会包含[1,n]条记录，所以需要循环
            System.out.println("\n RowKey:" + String.valueOf(Bytes.toInt(row.getRow())));
            printRow(row);
        }
    }

    public void printRow(Result row) {
        for (Cell cell : row.rawCells()) {
            System.out.print(String.valueOf(Bytes.toInt(cell.getRow())) + " ");
            System.out.print(new String(cell.getFamily()) + ":");
            System.out.print(new String(cell.getQualifier()) + " = ");
            System.out.print(new String(cell.getValue()));
            System.out.print("   Timestamp = " + cell.getTimestamp());
            System.out.println("");
        }
    }
}

