package com.dtstack.flinkx.connector.jdbc.source;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
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
public class JdbcDynamicTableSource
        implements LookupTableSource, SupportsProjectionPushDown {

    protected final SinkConnectionConf connectionConf;
    protected final LookupConf lookupConf;
    protected TableSchema physicalSchema;
    protected final String dialectName;
    protected final JdbcDialect jdbcDialect;

    public JdbcDynamicTableSource(
            SinkConnectionConf connectionConf,
            LookupConf lookupConf,
            TableSchema physicalSchema,
            JdbcDialect jdbcDialect
    ) {
        this.connectionConf = connectionConf;
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
                && Objects.equals(lookupConf, that.lookupConf)
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionConf, lookupConf, physicalSchema, dialectName);
    }
}
