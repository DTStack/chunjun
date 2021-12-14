package com.dtstack.flinkx.mapping;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/14 星期二
 */
public class MappingConf implements Serializable {

    private static final long serialVersionUID = 1L;

    private SchemaMapping schema;

    private TableMapping table;

    private FieldMapping field;

    public SchemaMapping getSchema() {
        return schema;
    }

    public void setSchema(SchemaMapping schema) {
        this.schema = schema;
    }

    public TableMapping getTable() {
        return table;
    }

    public void setTable(TableMapping table) {
        this.table = table;
    }

    public FieldMapping getField() {
        return field;
    }

    public void setField(FieldMapping field) {
        this.field = field;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MappingConf.class.getSimpleName() + "[", "]")
                .add("schema=" + schema)
                .add("table=" + table)
                .add("field=" + field)
                .toString();
    }
}
