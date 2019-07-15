package com.dtstack.flinkx.hbase.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author jingzhen
 */
public class HBaseUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseUtil.class);

    public static boolean checkConnection(Map<String,Object> map) {
        boolean check = false;
        Connection hConn = null;
        try {
            Map<String, Object> configWithTimeoutMap = addDefaultTimeoutConfig(map);
            hConn = getConnection(configWithTimeoutMap);
            ClusterStatus clusterStatus = hConn.getAdmin().getClusterStatus();
            check = true;
        } catch(Exception e) {
            e.printStackTrace();
            LOG.error("{}", e);
        } finally {
            closeConnection(hConn);
        }
        return check;
    }

    private static Map<String,Object> addDefaultTimeoutConfig(Map<String, Object> map) {
        Map<String,Object> paramMap = new HashMap<>();
        if (map == null) {
            return paramMap;
        }
        paramMap.putAll(map);
        paramMap.put("hbase.rpc.timeout", "60000");
        paramMap.put("ipc.socket.timeout", "20000");
        paramMap.put("hbase.client.retries.number", "3");
        paramMap.put("hbase.client.pause", "100");
        paramMap.put("zookeeper.recovery.retry", "3");
        return paramMap;
    }

    public static Connection getConnection(Map<String,Object> map) {
        Configuration hConfig = HBaseConfiguration.create();

        if(map == null || map.size() == 0) {
            throw new RuntimeException("hbase config can not be null");
        }

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            hConfig.set(entry.getKey(), (String) entry.getValue());
        }

        Connection hConn = null;
        try {
            hConn = ConnectionFactory.createConnection(hConfig);
            return hConn;
        } catch (Exception e) {
            throw new RuntimeException("获取 hbase 连接异常", e);
        }

    }

    public static List<String> getTableList(Map<String,Object> map) {
        Connection hConn = null;
        Admin admin = null;
        List<String> tableList = new ArrayList<>();
        try {
            hConn = getConnection(map);
            admin = hConn.getAdmin();
            TableName[] tableNames = admin.listTableNames();
            if(tableNames != null) {
                for(TableName tableName : tableNames) {
                    tableList.add(tableName.getNameAsString());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("获取 hbase table list 异常", e);
        } finally {
            closeAdmin(admin);
            closeConnection(hConn);
        }
        return tableList;
    }

    public static void closeConnection(Connection hConn) {
        if(hConn != null) {
            try {
                hConn.close();
            } catch (IOException e) {
                throw new RuntimeException("hbase 关闭连接异常", e);
            }
        }
    }

    public static void closeAdmin(Admin admin) {
        if(admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                throw new RuntimeException("hbase can not close admin error", e);
            }
        }
    }

    public static void closeTable(Table table) {
        if(table != null) {
            try {
                table.close();
            } catch (IOException e) {
                throw new RuntimeException("hbase can not close table error", e);
            }
        }
    }

    public static List<String> getColumnFamilyList(Map<String,Object> map, String table) {
        Connection hConn = null;
        Table tb = null;
        List<String> cfList = new ArrayList<>();

        try {
            hConn = getConnection(map);
            TableName tableName = TableName.valueOf(table);
            tb = hConn.getTable(tableName);
            HTableDescriptor hTableDescriptor = tb.getTableDescriptor();
            HColumnDescriptor[] columnDescriptors = hTableDescriptor.getColumnFamilies();
            for(HColumnDescriptor columnDescriptor: columnDescriptors) {
                cfList.add(columnDescriptor.getNameAsString());
            }
        } catch (IOException e) {
            throw new RuntimeException("hbase list column families error", e);
        } finally {
            closeTable(tb);
            closeConnection(hConn);
        }

        return cfList;
    }

}
