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
     * 默认值
     */
    private String  defaultValue;

    /**
     * 字段最大长度
     */
    private Integer length;

    /**
     * 是否非空字段
     */
    private Boolean isNullable;

    /**
     * 是否自增
     */
    private Boolean isAutoIncrement;

    /**
     * 字段在表中的位置，从1开始
     */
    private Integer position;

    /**
     * 注释
     */
    private String comment;

    public ColumnMetaData() {
    }

    public ColumnMetaData(String columnName, String dataType, String defaultValue, Integer length, Boolean isNullable, Boolean isAutoIncrement, Integer position, String comment) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.defaultValue = defaultValue;
        this.length = length;
        this.isNullable = isNullable;
        this.isAutoIncrement = isAutoIncrement;
        this.position = position;
        this.comment = comment;
    }


    @Override
    public String toString() {
        return "ColumnMetaData{" +
                "columnName='" + columnName + '\'' +
                ", dataType='" + dataType + '\'' +
                ", defaultValue='" + defaultValue + '\'' +
                ", length=" + length +
                ", isNullable=" + isNullable +
                ", isAutoIncrement=" + isAutoIncrement +
                ", position=" + position +
                ", comment='" + comment + '\'' +
                '}';
    }
}
