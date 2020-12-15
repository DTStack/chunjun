package com.dtstack.flinkx.metadatapostgresql.pojo;

/**
 * flinkx-all com.dtstack.flinkx.metadatapostgresql.pojo
 *
 * @author shitou
 * @description //字段的元数据
 * @date 2020/12/15 11:44
 */
public class ColumnMetaData {

    /**
     * 字段名
     */
    private String columnName;
    /**
     * 数据类型
     */
    private String  dataType;
    /**
     * 字段最大长度
     */
    private Integer length;

    /**
     * 是否非空字段
     */
    private Boolean isNotNull;
    /**
     * 注释
     */
    private String comment;

    public ColumnMetaData() {
    }

    public ColumnMetaData(String columnName, String dataType, Integer length, Boolean isNotNull, String comment) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.length = length;
        this.isNotNull = isNotNull;
        this.comment = comment;
    }

    @Override
    public String toString() {
        return "ColumnMetaData{" +
                "columnName='" + columnName + '\'' +
                ", dataType='" + dataType + '\'' +
                ", length=" + length +
                ", isNotNull=" + isNotNull +
                ", comment='" + comment + '\'' +
                '}';
    }
}
