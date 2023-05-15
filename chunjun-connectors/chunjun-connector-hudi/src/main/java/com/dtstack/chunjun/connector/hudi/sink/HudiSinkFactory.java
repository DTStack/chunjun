package com.dtstack.chunjun.connector.hudi.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.hudi.adaptor.HudiOnChunjunAdaptor;
import com.dtstack.chunjun.connector.hudi.converter.HudiRawTypeConvertor;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.java.typeutils.TypeExtractionException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;

import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/** chunjun's hudi async function implements the factory */
public class HudiSinkFactory extends SinkFactory {
    HudiOnChunjunAdaptor adaptor;

    public HudiSinkFactory(SyncConfig syncConf) {
        super(syncConf);
        Map<String, String> hudiConfig =
                (Map<String, String>) syncConf.getWriter().getParameter().get("hudiConfig");
        checkArgument(
                !StringUtils.isNullOrEmpty(hudiConfig.get(FlinkOptions.PATH.key())),
                "Option [path] should not be empty.");
        this.adaptor = new HudiOnChunjunAdaptor(syncConf, Configuration.fromMap(hudiConfig));
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return HudiRawTypeConvertor::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        //        RowType rowType = null;
        List<FieldConfig> fieldList = syncConfig.getWriter().getFieldList();
        ResolvedSchema schema = TableUtil.createTableSchema(fieldList, getRawTypeConverter());

        try {
            return adaptor.createHudiSinkDataStream(dataSet, schema);
        } catch (TypeExtractionException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }
}
