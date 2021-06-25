package com.dtstack.flinkx.connector.oracle.converter;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;

import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class BlobType extends LogicalType {


    private static final Class<?> INPUT_CONVERSION = byte[].class;

    private static final Class<?> DEFAULT_CONVERSION = byte[].class;

    private static final Set<String> OUTPUT_CONVERSION =
            conversionSet(
                    InputStream.class.getName());

    public BlobType(boolean isNullable, LogicalTypeRoot typeRoot) {
        super(isNullable, typeRoot);
    }


    @Override
    public String asSerializableString() {
        return "Clob-9953";
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_CONVERSION == clazz;
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
        return new BlobType(isNullable,getTypeRoot());
    }
}
