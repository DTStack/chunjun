package com.dtstack.flinkx.metadataes6.reader;


import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.metadataes6.constants.MetaDataEs6Cons;
import com.dtstack.flinkx.metadataes6.format.Metadataes6InputFormatBuilder;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Metadataes6Reader extends BaseDataReader {

    private String address;   //数据库地址

    private String username;

    private String password;

    private List<Object> indices;   //索引列表

    private Map<String,Object> clientConfig;

    public Metadataes6Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        address = readerConfig.getParameter().getStringVal(MetaDataEs6Cons.KEY_ADDRESS);
        username = readerConfig.getParameter().getStringVal(MetaDataEs6Cons.KEY_USERNAME);
        password = readerConfig.getParameter().getStringVal(MetaDataEs6Cons.KEY_PASSWORD);
        indices = (List<Object>) readerConfig.getParameter().getVal(MetaDataEs6Cons.KEY_INDICES);


        clientConfig = new HashMap<>();
        clientConfig.put(MetaDataEs6Cons.KEY_TIMEOUT, readerConfig.getParameter().getVal(MetaDataEs6Cons.KEY_TIMEOUT));
        clientConfig.put(MetaDataEs6Cons.KEY_PATH_PREFIX, readerConfig.getParameter().getVal(MetaDataEs6Cons.KEY_PATH_PREFIX));
    }

    @Override
    public DataStream<Row> readData() {
        Metadataes6InputFormatBuilder builder = new Metadataes6InputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setAddress(address);
        builder.setPassword(password);
        builder.setUsername(username);
        builder.setIndices(indices);
        builder.setClientConfig(clientConfig);

        BaseRichInputFormat format = builder.finish();

        return createInput(format);
    }

}
