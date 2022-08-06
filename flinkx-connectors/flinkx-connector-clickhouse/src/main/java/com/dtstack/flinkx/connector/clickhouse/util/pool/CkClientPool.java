package com.dtstack.flinkx.connector.clickhouse.util.pool;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.system.SystemUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.sql.Connection;

/**
 * @author piaochen.xing
 * @version $Id: CkClientPool.java, 2021-12-18 3:21 下午 xbkaishui Exp $$
 */
@Slf4j
public class CkClientPool {

    private GenericObjectPool<Connection> pool;

    private CkClientFactory clientFactory;

    public CkClientPool(CkClientFactory factory) {
        AbandonedConfig abandonedConfig = new AbandonedConfig();
        abandonedConfig.setRemoveAbandonedOnMaintenance(true); // 在Maintenance的时候检查是否有泄漏
        abandonedConfig.setRemoveAbandonedOnBorrow(true); // borrow 的时候检查泄漏
        abandonedConfig.setRemoveAbandonedTimeout(
                5 * 60); // 如果一个对象borrow之后5 * 60秒还没有返还给pool，认为是泄漏的对象

        this.pool = new GenericObjectPool<>(factory, factory.getCkProperties());
        this.pool.setAbandonedConfig(abandonedConfig);
        this.pool.setTestOnBorrow(true);
        this.pool.setTimeBetweenEvictionRunsMillis(60 * 1000); // 60秒运行一次维护任务
        int maxTool = (int) SystemUtil.getInt("ck.ds.pool", 20);
        int minIdle = (int) SystemUtil.getInt("ck.ds.minIdle", 5);
        this.pool.setMinIdle(minIdle);
        this.pool.setMaxTotal(maxTool);
        this.clientFactory = factory;
    }

    public void clear() {
        this.pool.clear();
    }

    /**
     * 创建connection对象
     *
     * @return
     */
    public Connection borrowObject() {
        try {
            Connection connection = pool.borrowObject();
            if (!connection.isValid(10)) {
                log.info("ck client reconnect ...");
            }
            return connection;
        } catch (Exception e) {
            log.error("get connection fail", e);
            return null;
        }
    }

    /**
     * 归还一个connection连接对象
     *
     * @param connection connection连接对象
     */
    public void returnObject(Connection connection) {
        if (ObjectUtil.isNotNull(connection)) {
            pool.returnObject(connection);
        }
    }
}
