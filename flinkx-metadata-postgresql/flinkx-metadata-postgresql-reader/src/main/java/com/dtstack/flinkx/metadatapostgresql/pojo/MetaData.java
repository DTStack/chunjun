package com.dtstack.flinkx.metadatapostgresql.pojo;

/**
 * flinkx-all com.dtstack.flinkx.metadatapostgresql.pojo
 *
 * @author shitou
 * @description //TODO
 * @date 2020/12/10 15:25
 */
public class MetaData {
    public String name;
    public String  datatype;
    public Integer length;
    public Boolean isnotnull;
    public String comment;

    public MetaData() {
    }

    public MetaData(String name, String datatype, Integer length, Boolean isnotnull, String comment) {
        this.name = name;
        this.datatype = datatype;
        this.length = length;
        this.isnotnull = isnotnull;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public Boolean getIsnotnull() {
        return isnotnull;
    }

    public void setIsnotnull(Boolean isnotnull) {
        this.isnotnull = isnotnull;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", dataType='" + datatype + '\'' +
                ", length=" + length +
                ", isNotNull=" + isnotnull +
                ", comment='" + comment + '\'' +
                '}';
    }
}
