package com.dtstack.flinkx.postgresql.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.postgresql.PostgresqlDatabaseMeta;
import com.dtstack.flinkx.postgresql.PostgresqlTypeConverter;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jiangbo
 * @date 2018/5/25 11:19
 */
public class PostgresqlReader extends JdbcDataReader {

    public PostgresqlReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        setDatabaseInterface(new PostgresqlDatabaseMeta());
        setTypeConverterInterface(new PostgresqlTypeConverter());
    }

    @Override
    public List<String> descColumnTypes(){
        List<String> columnType = new ArrayList<>();

        ClassUtil.forName(databaseInterface.getDriverClass(),this.getClass().getClassLoader());
        DriverManager.setLoginTimeout(10);
        Connection conn = null;

        try{
            conn = DriverManager.getConnection(dbUrl,username,password);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(databaseInterface.getSQLQueryColumnFields(null,table));

            while(rs.next()){
                String colName = rs.getString(2);
                String typeName = rs.getString(3);
                if(column.contains(colName)){
                    columnType.add(typeName);
                }
            }

        } catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            if(conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return columnType;
    }
}
