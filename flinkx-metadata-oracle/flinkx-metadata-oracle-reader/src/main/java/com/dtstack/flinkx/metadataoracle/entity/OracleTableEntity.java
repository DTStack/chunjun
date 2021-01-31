package com.dtstack.flinkx.metadataoracle.entity;

import com.dtstack.metadata.rdb.core.entity.TableEntity;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-27 17:43
 * @Description:
 */
public class OracleTableEntity extends TableEntity {

    private String tableType;

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }
}
