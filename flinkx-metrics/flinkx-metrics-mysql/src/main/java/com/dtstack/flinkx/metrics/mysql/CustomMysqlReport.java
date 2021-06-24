package com.dtstack.flinkx.metrics.mysql;

import com.dtstack.flinkx.conf.MetricParam;
import com.dtstack.flinkx.metrics.rdb.CustomRdbReporter;

public class CustomMysqlReport extends CustomRdbReporter {

    public CustomMysqlReport(MetricParam metricParam) {
        super(metricParam);
        super.jdbcDialect = new MysqlDialect();
    }

    @Override
    public void createTableIfNotExist() {

    }
}
