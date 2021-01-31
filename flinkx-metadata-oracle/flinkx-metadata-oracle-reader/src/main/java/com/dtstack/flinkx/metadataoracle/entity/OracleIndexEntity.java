package com.dtstack.flinkx.metadataoracle.entity;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-27 17:50
 * @Description:
 */
public class OracleIndexEntity {

    private String name;

    private String type;

    private String columnName;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
