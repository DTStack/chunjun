package com.dtstack.flinkx.oracle9.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.oracle9.Oracle9DatabaseMeta;
import com.dtstack.flinkx.oracle9.format.Oracle9OutputFormat;
import com.dtstack.flinkx.rdb.datawriter.JdbcDataWriter;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.constants.ConstantValue;
import org.apache.commons.lang3.StringUtils;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/30 17:31
 */
public class Oracle9Writer extends JdbcDataWriter {
    protected  String schema;
    public Oracle9Writer(DataTransferConfig config) {
        super(config);
        schema = config.getJob().getContent().get(0).getWriter().getParameter().getConnection().get(0).getSchema();
        if(StringUtils.isNotBlank(schema)){
            table = schema + ConstantValue.POINT_SYMBOL + table;
        }
        setDatabaseInterface(new Oracle9DatabaseMeta());
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        JdbcOutputFormatBuilder jdbcOutputFormatBuilder = new JdbcOutputFormatBuilder(new Oracle9OutputFormat());
        jdbcOutputFormatBuilder.setSchema(schema);
        return jdbcOutputFormatBuilder;
    }
}
