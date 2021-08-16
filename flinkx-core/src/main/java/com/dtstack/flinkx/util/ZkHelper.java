package com.dtstack.flinkx.util;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * ZooKeeper Util类
 *
 * @author kunni@dtstack.com
 */
public class ZkHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ZkHelper.class);

    public static final int DEFAULT_TIMEOUT = 5000;

    public static final String APPEND_PATH = "/table";

    public static final String DEFAULT_PATH = "/hbase";

    private ZkHelper() {}

    /**
     * @param hosts ip和端口
     * @param timeOut 创建超时时间
     */
    public static ZooKeeper createZkClient(String hosts, int timeOut) {
        try {
            LOG.info("try to create zookeeper client... ");
            return new ZooKeeper(hosts, timeOut, null, true);
        } catch (IOException e) {
            LOG.error(
                    "create zookeeper client failed. error {} ", ExceptionUtil.getErrorMessage(e));
            return null;
        }
    }

    /**
     * 获取某个节点的创建时间
     *
     * @param zooKeeper zookeeper
     * @param path 节点路径
     * @return 创建时间
     */
    public static long getCreateTime(ZooKeeper zooKeeper, String path) {
        Stat stat = new Stat();
        if (zooKeeper != null) {
            try {
                zooKeeper.getData(path, null, stat);
                return stat.getCtime();
            } catch (Exception e) {
                LOG.error(
                        "failed to get create time of {}, {}",
                        path,
                        ExceptionUtil.getErrorMessage(e));
                return 0L;
            }
        } else {
            return 0L;
        }
    }

    /**
     * 获取某个目录下的所有子节点
     *
     * @param zooKeeper zookeeper
     * @param path 目录
     * @return 子节点路径名集合
     */
    public static List<String> getChildren(ZooKeeper zooKeeper, String path) {
        if (zooKeeper != null) {
            try {
                return zooKeeper.getChildren(path, false);
            } catch (Exception e) {
                LOG.error(
                        "failed to get children, path :{}, {} ",
                        path,
                        ExceptionUtil.getErrorMessage(e));
                return null;
            }

        } else {
            return null;
        }
    }

    public static void closeZooKeeper(ZooKeeper zooKeeper) {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                LOG.error(ExceptionUtils.getMessage(e));
            }
        }
    }
}
