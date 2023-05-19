package com.dtstack.chunjun.connector.postgresql.converter.logical;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;

import org.postgresql.core.Oid;

import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class Float8Type extends PgCustomType {

    private static final Set<String> INPUT_CONVERSION =
            conversionSet(Double.class.getName(), Double.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = Double.class;

    private static final Set<String> OUTPUT_CONVERSION = conversionSet(Reader.class.getName());

    public Float8Type(boolean isNullable, LogicalTypeRoot typeRoot, boolean isArray) {
        super(isNullable, typeRoot, isArray, Oid.FLOAT8_ARRAY);
    }

    @Override
    public String asSerializableString() {
        return "PG-FLOAT8";
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return DEFAULT_CONVERSION;
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new Float8Type(isNullable, getTypeRoot(), isArray());
    }
}
