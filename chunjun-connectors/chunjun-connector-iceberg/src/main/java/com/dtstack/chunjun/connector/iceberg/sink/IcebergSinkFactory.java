package com.dtstack.chunjun.connector.iceberg.sink;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.iceberg.conf.IcebergConf;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.sink.WriteMode;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergSinkFactory extends SinkFactory {

    private IcebergConf icebergConf;

    public IcebergSinkFactory(SyncConf conf) {
        super(conf);
        icebergConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()), IcebergConf.class);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        List<String> columns =
                icebergConf.getColumn().stream()
                        .map(col -> col.getName())
                        .collect(Collectors.toList());

        DataStream<RowData> convertedDataStream =
                dataSet.map(new ChunjunRowDataConvertMap(icebergConf.getColumn()));

        boolean isOverwrite =
                icebergConf.getWriteMode().equalsIgnoreCase(WriteMode.OVERWRITE.name());
        return FlinkSink.forRowData(convertedDataStream)
                .tableLoader(buildTableLoader())
                .writeParallelism(icebergConf.getParallelism())
                .equalityFieldColumns(columns)
                .overwrite(isOverwrite)
                .build();
    }

    private TableLoader buildTableLoader() {
        Map<String, String> icebergProps = new HashMap<>();
        icebergProps.put("warehouse", icebergConf.getWarehouse());
        icebergProps.put("uri", icebergConf.getUri());

        /* build hadoop configuration */
        Configuration configuration = new Configuration();
        icebergConf
                .getHadoopConfig()
                .entrySet()
                .forEach(kv -> configuration.set(kv.getKey(), (String) kv.getValue()));

        CatalogLoader hc =
                CatalogLoader.hive(icebergConf.getDatabase(), configuration, icebergProps);
        TableLoader tl =
                TableLoader.fromCatalog(
                        hc, TableIdentifier.of(icebergConf.getDatabase(), icebergConf.getTable()));

        if (tl instanceof TableLoader.CatalogTableLoader) {
            tl.open();
        }

        return tl;
    }
}
