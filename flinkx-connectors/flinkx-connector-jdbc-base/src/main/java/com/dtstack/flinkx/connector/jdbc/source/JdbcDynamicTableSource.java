package com.dtstack.flinkx.connector.jdbc.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.dtstack.flinkx.lookup.options.LookupOptions;

import java.util.Objects;

/** A {@link DynamicTableSource} for JDBC. */
@Internal
abstract public class JdbcDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    protected final JdbcOptions options;
    protected final JdbcReadOptions readOptions;
    protected final LookupOptions lookupOptions;
    protected TableSchema physicalSchema;
    protected final String dialectName;

    public JdbcDynamicTableSource(
            JdbcOptions options,
            JdbcReadOptions readOptions,
            LookupOptions lookupOptions,
            TableSchema physicalSchema) {
        this.options = options;
        this.readOptions = readOptions;
        this.lookupOptions = lookupOptions;
        this.physicalSchema = physicalSchema;
        this.dialectName = options.getDialect().dialectName();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final JdbcRowDataInputFormat.Builder builder =
                JdbcRowDataInputFormat.builder()
                        .setDrivername(options.getDriverName())
                        .setDBUrl(options.getDbURL())
                        .setUsername(options.getUsername().orElse(null))
                        .setPassword(options.getPassword().orElse(null))
                        .setAutoCommit(readOptions.getAutoCommit());

        if (readOptions.getFetchSize() != 0) {
            builder.setFetchSize(readOptions.getFetchSize());
        }
        final JdbcDialect dialect = options.getDialect();
        String query =
                dialect.getSelectFromStatement(
                        options.getTableName(), physicalSchema.getFieldNames(), new String[0]);
        if (readOptions.getPartitionColumnName().isPresent()) {
            long lowerBound = readOptions.getPartitionLowerBound().get();
            long upperBound = readOptions.getPartitionUpperBound().get();
            int numPartitions = readOptions.getNumPartitions().get();
            builder.setParametersProvider(
                    new JdbcNumericBetweenParametersProvider(lowerBound, upperBound)
                            .ofBatchNum(numPartitions));
            query +=
                    " WHERE "
                            + dialect.quoteIdentifier(readOptions.getPartitionColumnName().get())
                            + " BETWEEN ? AND ?";
        }
        builder.setQuery(query);
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        builder.setRowConverter(dialect.getRowConverter(rowType));
        builder.setRowDataTypeInfo(
                runtimeProviderContext.createTypeInformation(physicalSchema.toRowDataType()));

        return InputFormatProvider.of(builder.build());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        // JDBC doesn't support nested projection
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JdbcDynamicTableSource)) {
            return false;
        }
        JdbcDynamicTableSource that = (JdbcDynamicTableSource) o;
        return Objects.equals(options, that.options)
                && Objects.equals(readOptions, that.readOptions)
                && Objects.equals(lookupOptions, that.lookupOptions)
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(options, readOptions, lookupOptions, physicalSchema, dialectName);
    }
}
