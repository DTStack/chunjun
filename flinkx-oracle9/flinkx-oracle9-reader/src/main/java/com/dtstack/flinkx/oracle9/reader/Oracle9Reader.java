package com.dtstack.flinkx.oracle9.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.oracle9.Oracle9DatabaseMeta;
import com.dtstack.flinkx.oracle9.format.Oracle9InputFormat;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormatBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/30 17:22
 */
public class Oracle9Reader extends JdbcDataReader {

    public Oracle9Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        String schema = config.getJob().getContent().get(0).getReader().getParameter().getConnection().get(0).getSchema();
        if(StringUtils.isNotBlank(schema)){
            table = schema + ConstantValue.POINT_SYMBOL + table;
        }
        setDatabaseInterface(new Oracle9DatabaseMeta());
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new Oracle9InputFormat());
    }
}
