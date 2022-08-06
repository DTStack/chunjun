package com.dtstack.flinkx.connector.clickhouse.util.pool;

import com.dtstack.flinkx.connector.clickhouse.util.CkProperties;

import cn.hutool.system.SystemUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author piaochen.xing
 * @version $Id: CkClientFactory.java, 2021-12-18 3:21 下午 xbkaishui Exp $$
 */
@Slf4j
public class CkClientFactory extends BasePooledObjectFactory<Connection> {

    private CkProperties ckProperties;

    @Override
    public Connection create() throws Exception {
        Properties prop = getCkProps();
        String clickhouseUrl = SystemUtil.get("clickhouse.url", ckProperties.getUrl());
        BalancedClickhouseDataSource balancedClickhouseDataSource =
                new BalancedClickhouseDataSource(clickhouseUrl, prop);
        int enableUrl = balancedClickhouseDataSource.actualize();
        while (enableUrl <= 0) {
            balancedClickhouseDataSource.actualize();
        }
        return balancedClickhouseDataSource.getConnection();
    }

    private Properties getCkProps() {
        Properties prop = new Properties();
        String password = SystemUtil.get("clickhouse.password", ckProperties.getPassword());
        String username = SystemUtil.get("clickhouse.username", ckProperties.getUsername());
        prop.put("driverClassName", ckProperties.getDriverClassName());
        prop.put("password", password);
        prop.put("username", username);
        return prop;
    }

    @Override
    public PooledObject<Connection> wrap(final Connection connection) {
        return new DefaultPooledObject<>(connection);
    }

    @Override
    public boolean validateObject(final PooledObject<Connection> p) {
        try {
            return p.getObject().isValid(10);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public void destroyObject(final PooledObject<Connection> p) throws Exception {
        try {
            Connection connection = p.getObject();
            connection.close();
        } catch (Exception e) {
            log.info("remove connection error ", e);
        }
    }

    public CkProperties getCkProperties() {
        return ckProperties;
    }

    public void setCkProperties(final CkProperties ckProperties) {
        this.ckProperties = ckProperties;
    }
}
