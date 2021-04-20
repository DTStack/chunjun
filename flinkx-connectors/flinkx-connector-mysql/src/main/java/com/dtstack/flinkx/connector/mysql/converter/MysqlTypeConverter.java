package com.dtstack.flinkx.connector.mysql.converter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.sql.SQLException;
import java.util.Locale;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/14
 **/
public class MysqlTypeConverter {

    /**
     * 将mysql数据库中的类型，转换成flink的DataType类型
     * MySQL支持的数据类型: com.mysql.jdbc.MysqlDefs
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
            case "BIT":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
            case "MEDIUMINT":
            case "INT":
            case "INTEGER":
            case "INT24":
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "REAL":
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DECIMAL":
            case "NUMERIC":
                // TODO 精度应该可以动态传进来？
                return DataTypes.DECIMAL(38, 18);
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "CHAR":
            case "VARCHAR":
                // TODO Flink还支持 DataTypes.VARCHAR(200)
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "YEAR":
                // TODO YEAR类型对应哪个DataType
                return DataTypes.DATE();
            case "TIMESTAMP":
            case "DATETIME":
                return DataTypes.TIMESTAMP();
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
                return DataTypes.BYTES();
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
                return DataTypes.STRING();
            // TODO ENUM、SET、GEOMETRY这三个得测试后才知道转成什么类型合适。
            case "ENUM":
            case "SET":
            case "GEOMETRY":
                return DataTypes.STRING();
            case "BINARY":
            case "VARBINARY":
                // BYTES 底层调用的是VARBINARY最大长度
                return DataTypes.BYTES();
            case "JSON":
                return DataTypes.STRING();

            default:
                throw new SQLException("不支持" + type + "类型");
        }
    }
}
