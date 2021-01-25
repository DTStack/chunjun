package com.dtstack.flinkx.metastore.entity;

import java.util.Map;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-05 10:58
 * @Description:
 */
public class MetaStoreClientInfo {

    private String metaStoreUrl;

    Map<String, Object> hiveConf;

    public String getMetaStoreUrl() {
        return metaStoreUrl;
    }

    public void setMetaStoreUrl(String metaStoreUrl) {
        this.metaStoreUrl = metaStoreUrl;
    }

    public Map<String, Object> getHiveConf() {
        return hiveConf;
    }

    public void setHiveConf(Map<String, Object> hiveConf) {
        this.hiveConf = hiveConf;
    }

    public MetaStoreClientInfo(String metaStoreUrl, Map<String, Object> hiveConf) {
        this.metaStoreUrl = metaStoreUrl;
        this.hiveConf = hiveConf;
    }
}
