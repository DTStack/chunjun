package com.dtstack.chunjun.connector.dm.converter.logical;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;

import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class BlobType extends LogicalType {

    private static final Set<String> INPUT_CONVERSION =
            conversionSet(String.class.getName(), byte[].class.getName());

    private static final Class<?> DEFAULT_CONVERSION = byte[].class;

    private static final Set<String> OUTPUT_CONVERSION = conversionSet(Reader.class.getName());

    public BlobType(boolean isNullable, LogicalTypeRoot typeRoot) {
        super(isNullable, typeRoot);
    }

    @Override
    public String asSerializableString() {
        return "DM-BLOB";
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
        return new BlobType(isNullable, getTypeRoot());
    }
}
