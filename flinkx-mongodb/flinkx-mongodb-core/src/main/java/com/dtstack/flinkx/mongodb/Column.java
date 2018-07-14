package com.dtstack.flinkx.mongodb;

import java.io.Serializable;

/**
 * @author jiangbo
 * @date 2018/7/3 14:24
 */
public class Column implements Serializable {

    private String name;

    private String type;

    private String splitter;

    public Column(String name, String type, String splitter) {
        this.name = name;
        this.type = type;
        this.splitter = splitter;
    }

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

    public String getSplitter() {
        return splitter;
    }

    public void setSplitter(String splitter) {
        this.splitter = splitter;
    }
}
