package com.dtstack.chunjun.connector.oceanbase.dialect;

import com.dtstack.chunjun.connector.mysql.converter.MysqlRawTypeConverter;
import com.dtstack.chunjun.connector.mysql.dialect.MysqlDialect;
import com.dtstack.chunjun.converter.RawTypeMapper;

import java.util.Optional;

public class OceanbaseMysqlModeDialect extends MysqlDialect {
    OceanbaseDialect oceanbaseDialect = new OceanbaseDialect();

    @Override
    public String dialectName() {
        return oceanbaseDialect.dialectName();
    }

    @Override
    public boolean canHandle(String url) {
        return oceanbaseDialect.canHandle(url);
    }

    @Override
    public RawTypeMapper getRawTypeConverter() {
        return MysqlRawTypeConverter::apply;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return oceanbaseDialect.defaultDriverName();
    }
}
