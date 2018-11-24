package com.dtstack.flinkx.carbondata.reader;

import com.dtstack.flinkx.carbondata.CarbonConfigKeys;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.DataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Carbondata reader plugin
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbondataReader extends DataReader {

    protected String table;

    protected String database;

    protected String path;

    protected Map<String,String> hadoopConfig;

    protected List<String> columnName;

    protected List<String> columnType;

    protected List<String> columnValue;


    public CarbondataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        hadoopConfig = (Map<String, String>) readerConfig.getParameter().getVal(CarbonConfigKeys.KEY_HADOOP_CONFIG);
        table = readerConfig.getParameter().getStringVal(CarbonConfigKeys.KEY_TABLE);
        database = readerConfig.getParameter().getStringVal(CarbonConfigKeys.KEY_DATABASE);
        path = readerConfig.getParameter().getStringVal(CarbonConfigKeys.KEY_TABLE_PATH);
        List columns = readerConfig.getParameter().getColumn();

        if (columns != null && columns.size() > 0) {
            if (columns.get(0) instanceof Map) {
                columnType = new ArrayList<>();
                columnValue = new ArrayList<>();
                columnName = new ArrayList<>();
                for(int i = 0; i < columns.size(); ++i) {
                    Map sm = (Map) columns.get(i);
                    columnType.add((String) sm.get("type"));
                    columnValue.add((String) sm.get("value"));
                    columnName.add((String) sm.get("name"));
                }
                System.out.println("init column finished");
            } else if (!columns.get(0).equals("*") || columns.size() != 1) {
                throw new IllegalArgumentException("column argument error");
            }
        } else {
            throw new IllegalArgumentException("column argument error");
        }
    }

    @Override
    public DataStream<Row> readData() {
        CarbondataInputFormatBuilder builder = new CarbondataInputFormatBuilder();
        builder.setColumnNames(columnName);
        builder.setColumnTypes(columnType);
        builder.setColumnValues(columnValue);
        builder.setDatabase(database);
        builder.setTable(table);
        builder.setPath(path);
        builder.setHadoopConfig(hadoopConfig);
        builder.setBytes(bytes);
        builder.setMonitorUrls(monitorUrls);
        return createInput(builder.finish(), "carbonreader");
    }

}