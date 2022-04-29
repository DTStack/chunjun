package com.dtstack.chunjun.connector.pgwal.util;

public class ColumnInfo {
    private final int index;
    private final String name;
    private final String type;
    private PostgresType postgresType;

    public ColumnInfo(int i, String name, String type) {
        this.index = i;
        this.name = name;
        this.type = type;
    }

    public int getIndex() {
        return index;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }
}
