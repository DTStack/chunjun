package com.dtstack.flinkx.connector.mysql;

import com.dtstack.flinkx.RawTypeConverter;
import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.JdbcLogicalTypeFactory;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.mysql.converter.MysqlTypeConverter;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/19
 **/
public class MysqlLogicalTypeFactory extends JdbcLogicalTypeFactory {

    public MysqlLogicalTypeFactory(JdbcConf jdbcConf, JdbcDialect jdbcDialect) {
        super(jdbcConf, jdbcDialect);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return MysqlTypeConverter::apply;
    }
}
