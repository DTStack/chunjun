package com.dtstack.flinkx.connector.jdbc.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.connector.jdbc.conf.SinkConnectionConf;
import com.dtstack.flinkx.connector.jdbc.lookup.JdbcAllTableFunction;
import com.dtstack.flinkx.connector.jdbc.lookup.JdbcLruTableFunction;
import com.dtstack.flinkx.enums.CacheType;
import com.dtstack.flinkx.lookup.conf.LookupConf;

import java.util.Objects;

/** A {@link DynamicTableSource} for JDBC. */
@Internal
public class JdbcDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    protected final SinkConnectionConf connectionConf;
    protected final JdbcReadOptions readOptions;
    protected final LookupConf lookupConf;
    protected TableSchema physicalSchema;
    protected final String dialectName;
    protected final JdbcDialect jdbcDialect;

    public JdbcDynamicTableSource(
            SinkConnectionConf connectionConf,
            JdbcReadOptions readOptions,
            LookupConf lookupConf,
            TableSchema physicalSchema,
            JdbcDialect jdbcDialect
    ) {
        this.connectionConf = connectionConf;
        this.readOptions = readOptions;
        this.lookupConf = lookupConf;
        this.physicalSchema = physicalSchema;
        this.jdbcDialect = jdbcDialect;
        this.dialectName = jdbcDialect.dialectName();
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
        }
        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        if (lookupConf.getCache().equalsIgnoreCase(CacheType.LRU.toString())) {
            return AsyncTableFunctionProvider.of(new JdbcLruTableFunction(
                    connectionConf,
                    jdbcDialect,
                    lookupConf,
                    physicalSchema.getFieldNames(),
                    keyNames,
                    rowType
            ));
        }
        return TableFunctionProvider.of(new JdbcAllTableFunction(
                connectionConf,
                jdbcDialect,
                lookupConf,
                physicalSchema.getFieldNames(),
                keyNames,
                rowType
        ));
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final JdbcRowDataInputFormat.Builder builder =
                JdbcRowDataInputFormat.builder()
                        .setDrivername(jdbcDialect.defaultDriverName().get())
                        .setDBUrl(connectionConf.obtainJdbcUrl())
                        .setUsername(connectionConf.getUsername())
                        .setPassword(connectionConf.getPassword())
                        .setAutoCommit(readOptions.getAutoCommit());

        if (readOptions.getFetchSize() != 0) {
            builder.setFetchSize(readOptions.getFetchSize());
        }
        String query =
                jdbcDialect.getSelectFromStatement(
                        connectionConf.obtainJdbcUrl(),
                        physicalSchema.getFieldNames(),
                        new String[0]);
        if (readOptions.getPartitionColumnName().isPresent()) {
            long lowerBound = readOptions.getPartitionLowerBound().get();
            long upperBound = readOptions.getPartitionUpperBound().get();
            int numPartitions = readOptions.getNumPartitions().get();
            builder.setParametersProvider(
                    new JdbcNumericBetweenParametersProvider(lowerBound, upperBound)
                            .ofBatchNum(numPartitions));
            query +=
                    " WHERE "
                            + jdbcDialect.quoteIdentifier(readOptions
                            .getPartitionColumnName()
                            .get())
                            + " BETWEEN ? AND ?";
        }
        builder.setQuery(query);
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        builder.setRowConverter(jdbcDialect.getRowConverter(rowType));
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
    public DynamicTableSource copy() {
        return new JdbcDynamicTableSource(
                connectionConf,
                readOptions,
                lookupConf,
                physicalSchema,
                jdbcDialect);
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
        return Objects.equals(connectionConf, that.connectionConf)
                && Objects.equals(readOptions, that.readOptions)
                && Objects.equals(lookupConf, that.lookupConf)
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionConf, readOptions, lookupConf, physicalSchema, dialectName);
    }
}
