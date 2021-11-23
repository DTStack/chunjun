package com.dtstack.flinkx.connector.dorisbatch.sink;

import com.dtstack.flinkx.connector.dorisbatch.converter.DorisRowConvert;
import com.dtstack.flinkx.connector.dorisbatch.options.DorisConf;
import com.dtstack.flinkx.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-11-21
 */
public class DorisDynamicTableSink implements DynamicTableSink {

    private final TableSchema physicalSchema;

    private final DorisConf dorisConf;

    public DorisDynamicTableSink(TableSchema physicalSchema, DorisConf dorisConf) {
        this.physicalSchema = physicalSchema;
        this.dorisConf = dorisConf;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        DorisOutputFormatBuilder builder = new DorisOutputFormatBuilder();
        builder.setRowConverter(new DorisRowConvert(rowType));
        builder.setDorisOptions(dorisConf);

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), dorisConf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new DorisDynamicTableSink(physicalSchema, dorisConf);
    }

    @Override
    public String asSummaryString() {
        return "doris sink";
    }
}
