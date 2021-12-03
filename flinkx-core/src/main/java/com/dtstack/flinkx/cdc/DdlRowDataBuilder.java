package com.dtstack.flinkx.cdc;

import com.dtstack.flinkx.element.column.StringColumn;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/3 星期五
 *     <p>构建DdlRowData，header顺序如下： tableIdentifier -> 0 | type -> 1 | sql -> 2 | lsn -> 3
 */
public class DdlRowDataBuilder {

    private static final String[] HEADERS = {"tableIdentifier", "type", "content", "lsn"};

    private final DdlRowData ddlRowData;

    private DdlRowDataBuilder() {
        ddlRowData = new DdlRowData(HEADERS);
    }

    public static DdlRowDataBuilder builder() {
        return new DdlRowDataBuilder();
    }

    public DdlRowDataBuilder setTableIdentifier(String tableIdentifier) {
        StringColumn tableIdentifierColumn = new StringColumn(tableIdentifier);
        ddlRowData.setColumn(0, tableIdentifierColumn);
        return this;
    }

    public DdlRowDataBuilder setType(String type) {
        StringColumn typeColumn = new StringColumn(type);
        ddlRowData.setColumn(1, typeColumn);
        return this;
    }

    public DdlRowDataBuilder setContent(String content) {
        StringColumn contentColumn = new StringColumn(content);
        ddlRowData.setColumn(2, contentColumn);
        return this;
    }

    public DdlRowDataBuilder setLsn(String lsn) {
        StringColumn lsnColumn = new StringColumn(lsn);
        ddlRowData.setColumn(3, lsnColumn);
        return this;
    }

    public DdlRowData build() {
        return ddlRowData;
    }
}
