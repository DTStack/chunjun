package com.dtstack.flinkx.connector.inceptor.source;

import com.dtstack.flinkx.connector.inceptor.conf.InceptorConf;
import com.dtstack.flinkx.connector.inceptor.lookup.InceptorAllTableFunction;
import com.dtstack.flinkx.connector.inceptor.lookup.InceptorLruTableFunction;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.source.JdbcDynamicTableSource;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.flinkx.enums.CacheType;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.table.connector.source.ParallelAsyncTableFunctionProvider;
import com.dtstack.flinkx.table.connector.source.ParallelTableFunctionProvider;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

/**
 * @author dujie @Description TODO
 * @createTime 2022-01-20 04:32:00
 */
public class InceptorDynamicTableSource extends JdbcDynamicTableSource {
    private final InceptorConf inceptorConf;

    public InceptorDynamicTableSource(
            InceptorConf inceptorConf,
            LookupConf lookupConf,
            TableSchema physicalSchema,
            JdbcDialect jdbcDialect,
            JdbcInputFormatBuilder builder) {
        super(inceptorConf, lookupConf, physicalSchema, jdbcDialect, builder);
        this.inceptorConf = inceptorConf;
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
            return ParallelAsyncTableFunctionProvider.of(
                    new InceptorLruTableFunction(
                            inceptorConf,
                            jdbcDialect,
                            lookupConf,
                            physicalSchema.getFieldNames(),
                            keyNames,
                            rowType),
                    lookupConf.getParallelism());
        }
        return ParallelTableFunctionProvider.of(
                new InceptorAllTableFunction(
                        inceptorConf,
                        jdbcDialect,
                        lookupConf,
                        physicalSchema.getFieldNames(),
                        keyNames,
                        rowType),
                lookupConf.getParallelism());
    }
}
