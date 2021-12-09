package com.dtstack.flinkx.cdc;

/**
 * 构建DdlRowData，header顺序如下： tableIdentifier -> 0 | type -> 1 | sql -> 2 | lsn -> 3
 *
 * @author tiezhu@dtstack.com
 * @since 2021/12/3 星期五
 */
public class DdlRowDataBuilder {

    private static final String[] HEADERS = {"tableIdentifier", "type", "content", "lsn"};

    private final DdlRowData ddlRowData;

    private String tableName;

    private String databaseName;

    private DdlRowDataBuilder() {
        ddlRowData = new DdlRowData(HEADERS);
    }

    public static DdlRowDataBuilder builder() {
        return new DdlRowDataBuilder();
    }

    public DdlRowDataBuilder setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public DdlRowDataBuilder setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public DdlRowDataBuilder setTableIdentifier(String tableIdentifier) {
        ddlRowData.setDdlInfo(0, tableIdentifier);
        return this;
    }

    public DdlRowDataBuilder setType(String type) {
        ddlRowData.setDdlInfo(1, type);
        return this;
    }

    public DdlRowDataBuilder setContent(String content) {
        ddlRowData.setDdlInfo(2, content);
        return this;
    }

    public DdlRowDataBuilder setLsn(String lsn) {
        ddlRowData.setDdlInfo(3, lsn);
        return this;
    }

    public DdlRowData build() {
        if (tableName != null && databaseName != null) {
            String tableIdentifier = "'" + databaseName + "'.'" + tableName + "'";
            ddlRowData.setDdlInfo(0, tableIdentifier);
        }
        return ddlRowData;
    }
}
