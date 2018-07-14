package com.dtstack.flinkx.writer;

/**
 * @author jiangbo
 * @date 2018/6/6 14:05
 */
public enum WriteMode {

    INSERT("insert"),

    UPDATE("update"),

    REPLACE("replace");

    private String mode;

    WriteMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }
}
