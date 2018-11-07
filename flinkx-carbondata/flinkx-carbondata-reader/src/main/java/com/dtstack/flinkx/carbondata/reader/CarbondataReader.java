package com.dtstack.flinkx.carbondata.reader;

import com.dtstack.flinkx.carbondata.CarbondataDatabaseMeta;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Carbondata reader plugin
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbondataReader extends JdbcDataReader {

    public CarbondataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        setDatabaseInterface(new CarbondataDatabaseMeta());
    }

}