package com.dtstack.flinkx.metadataphoenix5.entity;


import com.dtstack.metadata.rdb.core.entity.ColumnEntity;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-20 16:51
 * @Description:
 */
public class Phoenix5ColumnEntity extends ColumnEntity {

    /**是否是主键*/
    private String isPrimaryKey;

    public void setIsPrimaryKey(String isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }
}
