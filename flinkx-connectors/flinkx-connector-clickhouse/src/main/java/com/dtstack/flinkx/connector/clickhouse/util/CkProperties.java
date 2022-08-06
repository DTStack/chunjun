package com.dtstack.flinkx.connector.clickhouse.util;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.sql.Connection;

/**
 * @author piaochen.xing
 * @version $Id: CkProperties.java, 2021-12-18 3:31 下午 xbkaishui Exp $$
 */
@Slf4j
@Data
public class CkProperties extends GenericObjectPoolConfig<Connection> {

    private String url;

    private String driverClassName = "com.github.housepower.jdbc.ClickHouseDriver";

    private String username = "default";

    private String password = "cktest$R";
}
