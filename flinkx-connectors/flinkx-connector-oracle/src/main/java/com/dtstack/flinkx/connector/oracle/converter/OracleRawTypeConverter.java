package com.dtstack.flinkx.connector.oracle.converter;

import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.sql.SQLException;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleRawTypeConverter {

    private final static String TIMESTAMP = "^TIMESTAMP\\(\\d+\\)";
    private final static Predicate<String> TIMESTAMP_PREDICATE = Pattern.compile(TIMESTAMP).asPredicate();


    /**
     * 将Oracle数据库中的类型，转换成flink的DataType类型。
     *
     * @param type
     *
     * @return
     *
     * @throws SQLException
     */
    public static DataType apply(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "FLOAT":
            case "REAL":
            case "BINARY_DOUBLE":
                return DataTypes.DOUBLE();
            case "CHAR":
            case "VARCHAR":
            case "VARCHAR2":
            case "NCHAR":
            case "LONG":
            case "NVARCHAR2":
                return DataTypes.STRING();
            case "INT":
            case "INTEGER":
            case "NUMBER":
            case "DECIMAL":
                return DataTypes.DECIMAL(1, 0);
            case "DATE":
                return DataTypes.TIMESTAMP();
            case "RAW":
            case "LONG RAW":
                return DataTypes.BYTES();
            case "BINARY_FLOAT":
                return DataTypes.FLOAT();
            case "BLOB":
                throw new UnsupportedTypeException(type);
            default:
                if (TIMESTAMP_PREDICATE.test(type)) {
                    return DataTypes.TIMESTAMP();
                }else if(type.startsWith("INTERVAL")){
                    return DataTypes.STRING();
                }
                throw new UnsupportedTypeException(type);
        }
    }


}
