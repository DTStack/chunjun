package com.dtstack.flinkx.s3.reader;

import com.amazonaws.services.s3.AmazonS3;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.s3.S3Util;
import com.dtstack.flinkx.s3.S3Config;
import com.dtstack.flinkx.s3.format.S3InputFormat;
import com.dtstack.flinkx.s3.format.S3InputFormatBuilder;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class S3Reader extends BaseDataReader{

    private S3Config s3Config;
    private List<MetaColumn> metaColumns;

    public S3Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

        try {
            String s3json = objectMapper.writeValueAsString(readerConfig.getParameter().getAll());
            s3Config = objectMapper.readValue(s3json, S3Config.class);
        } catch (Exception e) {
            throw new RuntimeException("解析S3Config配置出错:", e);
        }


        List columns = readerConfig.getParameter().getColumn();
        metaColumns = MetaColumn.getMetaColumns(columns, false);
    }

    @Override
    public DataStream<Row> readData() {
        S3InputFormatBuilder builder = new S3InputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setS3Config(s3Config);
        builder.setMetaColumn(metaColumns);
        builder.setRestoreConfig(restoreConfig);
        BaseRichInputFormat inputFormat = builder.finish();
        resolveObjects((S3InputFormat) inputFormat);
        return createInput(inputFormat);
    }

    public void resolveObjects(S3InputFormat s3InputFormat) {
        S3Config s3Config = s3InputFormat.getS3Config();
        String bucket = s3Config.getBucket();
        Set<String> resolved = new TreeSet<>();
        AmazonS3 amazonS3 = S3Util.initS3(s3Config);
        for (String path : s3Config.getObject()) {
            int index = path.indexOf("/*");
            if (index > 0) {
                String prefix = path.substring(0, index + 1);
                List<String> subObjects = S3Util.listObjects(amazonS3, bucket, prefix);
                resolved.addAll(subObjects);
            } else {
                // todo 是否有必要检验 object 的 key 是否存在
                if(S3Util.doesObjectExist(amazonS3,bucket,path)) {
                    resolved.add(path);
                }
            }
        }
        List<String> distinct = new ArrayList<>(resolved);
        s3InputFormat.setObjects(distinct);
    }
}
