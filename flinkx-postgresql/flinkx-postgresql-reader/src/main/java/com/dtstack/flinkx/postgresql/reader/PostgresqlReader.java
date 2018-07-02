package com.dtstack.flinkx.postgresql.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.postgresql.PostgresqlDatabaseMeta;
import com.dtstack.flinkx.postgresql.PostgresqlTypeConverter;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.rdb.util.DBUtil;
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
        Connection conn;
        try{
            ClassUtil.forName(databaseInterface.getDriverClass(),getClass().getClassLoader());
            conn = DBUtil.getConnection(dbUrl,username,password);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(databaseInterface.getSQLQueryColumnFields(null,table));

            while(rs.next()){
                String colName = rs.getString(2);
                String typeName = rs.getString(3);
                if(column.contains(colName)){
                    columnType.add(typeName);
                }
            }

            DBUtil.closeDBResources(rs,stmt,conn);
        } catch (Exception e){
            throw new RuntimeException(e);
        }

        return columnType;
    }
}
