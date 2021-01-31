package com.dtstack.flinkx.metadataoracle.entity;

import com.dtstack.metadata.rdb.core.entity.ColumnEntity;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-27 17:59
 * @Description:
 */
public class OracleColumnEntity extends ColumnEntity {

    private String primaryKey;

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }
}
