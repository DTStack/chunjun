package com.dtstack.flinkx.connector.oracle.util;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.connector.oracle.OracleDialect;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleUtil {

//    public static void analyzeOracleMetaData(JdbcConf jdbcConf, JdbcDialect jdbcDialect, List<String> column,List<String> columnType){
//        String[] fieldNames = jdbcConf
//                .getColumn()
//                .stream()
//                .map(FieldConf::getName)
//                .toArray(String[]::new);
//        OracleDialect oracleDialect = (OracleDialect) jdbcDialect;
//        String metaDataSql = oracleDialect.queryOracleMetaDataStatement(jdbcConf.getSchema(),jdbcConf.getTable(), fieldNames);
//
//        try (Connection dbConn = JdbcUtil.getConnection(jdbcConf, jdbcDialect);
//             Statement statement = dbConn.createStatement()) {
//            ResultSet resultSet = statement.executeQuery(metaDataSql);
//            while (resultSet.next()) {
//                int precision = resultSet.getInt(1);
//                int scale = resultSet.getInt(2);
//                String rawName = resultSet.getString(3);
//                String rawType = resultSet.getString(4);
//                column.add(rawName);
//                columnType.add(rawType+"("+precision+","+scale+")");
//            }
//        } catch (SQLException throwables) {
//            throwables.printStackTrace();
//        }
//    }
}
