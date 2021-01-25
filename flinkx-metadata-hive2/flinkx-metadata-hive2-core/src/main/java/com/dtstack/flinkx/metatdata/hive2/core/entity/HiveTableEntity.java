package com.dtstack.flinkx.metatdata.hive2.core.entity;


import com.dtstack.metadata.rdb.core.entity.TableEntity;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-20 11:06
 * @Description:
 */
public class HiveTableEntity extends TableEntity {

    private String lastAccessTime;

    private String transientLastDdlTime;

    private String location;

    private String storeType;

    public void setLastAccessTime(String lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    public void setTransientLastDdlTime(String transientLastDdlTime) {
        this.transientLastDdlTime = transientLastDdlTime;
    }
    public void setLocation(String location) {
        this.location = location;
    }
}
