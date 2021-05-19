package com.dtstack.flinkx.connector.oracle.converter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.sql.SQLException;
import java.util.Locale;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/14
 **/
public class OracleRawTypeConverter {

    /**
     * 将MySQL数据库中的类型，转换成flink的DataType类型。
     * 转换关系参考 com.mysql.jdbc.MysqlDefs 类里面的信息。
     * com.mysql.jdbc.ResultSetImpl.getObject(int)
     *
     * @param type
     *
     * @return
     *
     * @throws SQLException
     */
    public static DataType apply(String type) throws SQLException {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "SMALLINT" :
                return DataTypes.SMALLINT();
            case "FLOAT":
            case "REAL":
            case "BINARY_DOUBLE":
                return DataTypes.DOUBLE();
            case "CHAR" :
            case "VARCHAR":
            case "VARCHAR2":
            case "NCHAR":
            case "LONG":
            case "NVARCHAR2":
                return DataTypes.STRING();
            case "INT":
            case "INTEGER":
            case "NUMBER":
            case "DECIMAL" :
                return DataTypes.DECIMAL(1,0);
            case "DATE":
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();
            case "RAW":
            case "LONG RAW":
                return DataTypes.BYTES();
            case "BINARY_FLOAT":
                return DataTypes.FLOAT();
            case "BLOB":
                throw new SQLException("不支持" + type + "类型");
            default:
                if(type.startsWith("TIMESTAMP")){
                    return DataTypes.TIMESTAMP();
                }
                throw new SQLException("不支持" + type + "类型");
        }
    }
}
