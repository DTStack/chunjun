package com.dtstack.flinkx.connector.dorisbatch.table;

import com.dtstack.flinkx.connector.dorisbatch.options.DorisConf;
import com.dtstack.flinkx.connector.dorisbatch.options.DorisOptions;
import com.dtstack.flinkx.connector.dorisbatch.options.LoadConf;
import com.dtstack.flinkx.connector.dorisbatch.options.LoadConfBuilder;
import com.dtstack.flinkx.connector.dorisbatch.sink.DorisDynamicTableSink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * declare doris table factory info.
 *
 * <p>Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-11-21
 */
public class DorisDynamicTableFactory implements DynamicTableSinkFactory {

    private static final String IDENTIFIER = "doris-x";

    private static final Set<ConfigOption<?>> requiredOptions =
            Stream.of(DorisOptions.FENODES, DorisOptions.TABLE_IDENTIFY)
                    .collect(Collectors.toSet());

    private static final Set<ConfigOption<?>> optionalOptions =
            Stream.of(
                            DorisOptions.USERNAME,
                            DorisOptions.PASSWORD,
                            DorisOptions.REQUEST_TABLET_SIZE,
                            DorisOptions.REQUEST_CONNECT_TIMEOUT_MS,
                            DorisOptions.REQUEST_READ_TIMEOUT_MS,
                            DorisOptions.REQUEST_QUERY_TIMEOUT_SEC,
                            DorisOptions.REQUEST_RETRIES,
                            DorisOptions.REQUEST_BATCH_SIZE,
                            DorisOptions.EXEC_MEM_LIMIT,
                            DorisOptions.DESERIALIZE_QUEUE_SIZE,
                            DorisOptions.DESERIALIZE_ARROW_ASYNC,
                            DorisOptions.FIELD_DELIMITER,
                            DorisOptions.LINE_DELIMITER,
                            DorisOptions.MAX_RETRIES,
                            DorisOptions.WRITE_MODE,
                            DorisOptions.BATCH_SIZE)
                    .collect(Collectors.toSet());

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();

        // 3.封装参数
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        DorisConf ftpConfig = getConfByOptions(config);
        return new DorisDynamicTableSink(physicalSchema, ftpConfig);
    }

    private static DorisConf getConfByOptions(ReadableConfig config) {
        DorisConf dorisConf = new DorisConf();

        dorisConf.setFeNodes(config.get(DorisOptions.FENODES));
        String tableIdentify = config.get(DorisOptions.TABLE_IDENTIFY);
        if (tableIdentify.contains(".")) {
            String[] identifyInfo = tableIdentify.split("\\.");
            dorisConf.setDatabase(identifyInfo[0]);
            dorisConf.setTable(identifyInfo[1]);
        }

        if (config.get(DorisOptions.USERNAME) != null) {
            dorisConf.setUsername(config.get(DorisOptions.USERNAME));
        }

        if (config.get(DorisOptions.PASSWORD) != null) {
            dorisConf.setPassword(config.get(DorisOptions.PASSWORD));
        }

        LoadConf loadConf = getLoadConf(config);
        dorisConf.setLoadConf(loadConf);
        dorisConf.setFieldDelimiter(config.get(DorisOptions.FIELD_DELIMITER));
        dorisConf.setLineDelimiter(config.get(DorisOptions.LINE_DELIMITER));
        dorisConf.setLoadProperties(new Properties());
        dorisConf.setMaxRetries(config.get(DorisOptions.MAX_RETRIES));
        dorisConf.setWriteMode(config.get(DorisOptions.WRITE_MODE));
        dorisConf.setBatchSize(config.get(DorisOptions.BATCH_SIZE));

        return dorisConf;
    }

    private static LoadConf getLoadConf(ReadableConfig config) {
        LoadConfBuilder loadConfBuilder = new LoadConfBuilder();
        return loadConfBuilder
                .setRequestTabletSize(config.get(DorisOptions.REQUEST_TABLET_SIZE))
                .setRequestConnectTimeoutMs(config.get(DorisOptions.REQUEST_CONNECT_TIMEOUT_MS))
                .setRequestReadTimeoutMs(config.get(DorisOptions.REQUEST_READ_TIMEOUT_MS))
                .setRequestQueryTimeoutMs(config.get(DorisOptions.REQUEST_QUERY_TIMEOUT_SEC))
                .setRequestRetries(config.get(DorisOptions.REQUEST_RETRIES))
                .setRequestBatchSize(config.get(DorisOptions.REQUEST_BATCH_SIZE))
                .setExecMemLimit(config.get(DorisOptions.EXEC_MEM_LIMIT))
                .setDeserializeQueueSize(config.get(DorisOptions.DESERIALIZE_QUEUE_SIZE))
                .setDeserializeArrowAsync(config.get(DorisOptions.DESERIALIZE_ARROW_ASYNC))
                .build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return optionalOptions;
    }
}
