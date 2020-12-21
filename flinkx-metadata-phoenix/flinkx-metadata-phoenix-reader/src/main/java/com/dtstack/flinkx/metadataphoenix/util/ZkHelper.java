package com.dtstack.flinkx.metadataphoenix.util;

import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * ZooKeeper Util类
 * @author kunni@dtstack.com
 */
public class ZkHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ZkHelper.class);

    public static final int DEFAULT_TIMEOUT = 20000;

    public static final String DEFAULT_PATH = "/hbase/table";

    private static ZooKeeper zooKeeper;

    private ZkHelper(){}

    /**
     * 单例模式, 确保不会重复创建连接
     * @param hosts ip和端口
     * @param timeOut 创建超时时间
     */
    public static void createSingleZkClient(String hosts, int timeOut) {
        if(zkAvailable()){
            ZkHelper.closeZooKeeper();
        }
        try {
            zooKeeper = new ZooKeeper(hosts, timeOut, null);
            LOG.info("create zookeeper client success ");
        }catch (IOException e){
            zooKeeper = null;
            LOG.error("create zookeeper client failed. error {} ", ExceptionUtil.getErrorMessage(e));
        }
    }

    public static long getStat(String path) throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        if(zkAvailable()){
            zooKeeper.getData(path, null, stat);
            return stat.getCtime();
        }else {
            return 0L;
        }
    }

    public static List<String> getChildren(String path) throws KeeperException, InterruptedException {
        if(zkAvailable()){
            return zooKeeper.getChildren(path,false);
        }else {
            return null;
        }

    }

    public static void closeZooKeeper() {
        if(zkAvailable()){
            try{
                zooKeeper.close();
            }catch (InterruptedException e){
                LOG.error(ExceptionUtils.getMessage(e));
            }
        }
    }

    public static boolean zkAvailable(){
        return zooKeeper != null;
    }

}
