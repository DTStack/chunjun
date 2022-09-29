package com.dtstack.chunjun.cdc;

/**
 * 构建DdlRowData，header顺序如下： database -> 0 | schema -> 1 | table ->2 | type -> 3 | sql -> 4 | lsn ->
 * 5 | lsn_sequence -> 6
 *
 * @author tiezhu@dtstack.com
 * @since 2021/12/3 星期五
 */
public class DdlRowDataBuilder {

    private static final String[] HEADERS = {
        "database", "schema", "table", "type", "content", "lsn", "lsn_sequence", "snapshot"
    };

    private final DdlRowData ddlRowData;

    private DdlRowDataBuilder() {
        ddlRowData = new DdlRowData(HEADERS);
    }

    public static DdlRowDataBuilder builder() {
        return new DdlRowDataBuilder();
    }

    public DdlRowDataBuilder setDatabaseName(String databaseName) {
        ddlRowData.setDdlInfo(0, databaseName);
        return this;
    }

    public DdlRowDataBuilder setSchemaName(String schemaName) {
        ddlRowData.setDdlInfo(1, schemaName);
        return this;
    }

    public DdlRowDataBuilder setTableName(String tableName) {
        ddlRowData.setDdlInfo(2, tableName);
        return this;
    }

    public DdlRowDataBuilder setType(String type) {
        ddlRowData.setDdlInfo(3, type);
        return this;
    }

    public DdlRowDataBuilder setContent(String content) {
        ddlRowData.setDdlInfo(4, content);
        return this;
    }

    public DdlRowDataBuilder setLsn(String lsn) {
        ddlRowData.setDdlInfo(5, lsn);
        return this;
    }

    public DdlRowDataBuilder setLsnSequence(String lsnSequence) {
        ddlRowData.setDdlInfo(6, lsnSequence);
        return this;
    }

    public DdlRowDataBuilder setSnapShot(Boolean snapShot) {
        ddlRowData.setDdlInfo(7, snapShot.toString());
        return this;
    }

    public DdlRowData build() {
        return ddlRowData;
    }
}
