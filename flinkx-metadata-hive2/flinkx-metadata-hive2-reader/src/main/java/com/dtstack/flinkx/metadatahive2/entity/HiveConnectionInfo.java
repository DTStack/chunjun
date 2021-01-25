package com.dtstack.flinkx.metadatahive2.entity;


import com.dtstack.metadata.rdb.core.entity.ConnectionInfo;

import java.util.Map;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-20 14:57
 * @Description:
 */
public class HiveConnectionInfo extends ConnectionInfo {

    public HiveConnectionInfo(ConnectionInfo connectionInfo) {
        super(connectionInfo);
    }

    private Map<String, Object> hiveConf;

    public Map<String, Object> getHiveConf() {
        return hiveConf;
    }

    public void setHiveConf(Map<String, Object> hiveConf) {
        this.hiveConf = hiveConf;
    }
}
