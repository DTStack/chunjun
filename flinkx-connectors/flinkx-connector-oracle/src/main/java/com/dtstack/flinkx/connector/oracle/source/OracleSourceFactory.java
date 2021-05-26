package com.dtstack.flinkx.connector.oracle.source;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.flinkx.connector.oracle.OracleDialect;
import com.dtstack.flinkx.connector.oracle.converter.OracleRawTypeConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleSourceFactory extends JdbcSourceFactory {
    // 默认是流式拉取
    private static final int DEFAULT_FETCH_SIZE = Integer.MIN_VALUE;

    public OracleSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        super.jdbcDialect = new OracleDialect();
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new OracleInputFormat());
    }

    @Override
    protected int getDefaultFetchSize() {
        return DEFAULT_FETCH_SIZE;
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return OracleRawTypeConverter::apply;
    }
}
