package com.dtstack.flinkx.connector.elasticsearch5.sink;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.dtstack.flinkx.connector.elasticsearch5.conf.ElasticsearchConf;
import com.dtstack.flinkx.connector.elasticsearch5.converter.ElasticsearchRowConverter;
import com.dtstack.flinkx.streaming.api.functions.sink.DtOutputFormatSinkFunction;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/27 23:50
 */
public class ElasticsearchDynamicTableSink implements DynamicTableSink {
    private final TableSchema physicalSchema;
    private final ElasticsearchConf elasticsearchConf;

    public ElasticsearchDynamicTableSink(TableSchema physicalSchema, ElasticsearchConf elasticsearchConf) {
        this.physicalSchema = physicalSchema;
        this.elasticsearchConf = elasticsearchConf;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        ElasticsearchOutputFormatBuilder builder = new ElasticsearchOutputFormatBuilder();
        builder.setConverter(new ElasticsearchRowConverter(rowType));
        builder.setEsConf(elasticsearchConf);

        return SinkFunctionProvider.of(new DtOutputFormatSinkFunction<>(builder.finish()),
                1);
    }

    @Override
    public DynamicTableSink copy() {
        return new ElasticsearchDynamicTableSink(physicalSchema,
                elasticsearchConf);
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch6 sink";
    }

}
