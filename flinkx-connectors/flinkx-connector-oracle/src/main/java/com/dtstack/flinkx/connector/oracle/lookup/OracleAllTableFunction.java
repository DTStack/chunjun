package com.dtstack.flinkx.connector.oracle.lookup;

import com.dtstack.flinkx.connector.jdbc.lookup.JdbcAllTableFunction;

import com.dtstack.flinkx.factory.DTThreadFactory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.lookup.AbstractAllTableFunction;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.util.DtStringUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
@Internal
public class OracleAllTableFunction extends JdbcAllTableFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OracleAllTableFunction.class);


    public OracleAllTableFunction(
            JdbcConf jdbcConf,
            JdbcDialect jdbcDialect,
            LookupConf lookupConf,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        super(jdbcConf, jdbcDialect, lookupConf, fieldNames, keyNames, rowType);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", "true");
        super.open(context);
    }
}
