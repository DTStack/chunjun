package com.dtstack.chunjun.connector.postgresql.converter.logical;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

public abstract class PgCustomType extends LogicalType {

    private final boolean isArray;
    private final int arrayOid;

    public PgCustomType(
            boolean isNullable, LogicalTypeRoot typeRoot, boolean isArray, Integer oid) {
        super(isNullable, typeRoot);
        this.isArray = isArray;
        this.arrayOid = oid;
    }

    public boolean isArray() {
        return isArray;
    }

    public int getArrayOid() {
        return arrayOid;
    }
}
