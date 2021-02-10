package com.dtstack.flinkx.metadatahbase.entity;

import java.io.Serializable;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-18 19:38
 * @Description:
 */
public class HbaseColumnEntity implements Serializable {

    private String columnFamily;

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }
}
