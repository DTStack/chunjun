package com.dtstack.flinkx.connector.oracle.converter;

import oracle.jdbc.OracleType;

import org.apache.commons.lang3.StringUtils;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.sql.SQLException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/14
 **/
public class OracleRawTypeConverter {

    private static final String REGEX = "(?<name>[a-zA-Z]+)(?:\\(\\d+\\))?(?:\\((?<ps>\\d+(?:,-?\\d+)?)\\))?";
    private static final Pattern PATTERN = Pattern.compile(REGEX);

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
//        Tuple3<String,Integer,Integer> typeInfo = typeInfo(type);
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "SMALLINT" :
                return DataTypes.SMALLINT();
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "FLOAT":
            case "REAL":
                return DataTypes.DOUBLE();
            case "CHAR" :
            case "VARCHAR":
            case "VARCHAR2":
                return DataTypes.STRING();
            case "NUMBER":
            case "DECIMAL" :
                return DataTypes.DECIMAL(1,0);
            case "DATE":
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();
            default:
                if(type.startsWith("TIMESTAMP")){
                    return DataTypes.TIMESTAMP();
                }
                throw new SQLException("不支持" + type + "类型");
        }
    }

    public static Tuple3<String, Integer,Integer> typeInfo(String type){
        Matcher matcher = PATTERN.matcher(type);
        if(matcher.find()){
            String name = matcher.group("name");
            String ps = matcher.group("ps");
            if(StringUtils.isNotBlank(ps)){
                if(ps.contains(",")){
                    String[] typeInfo = ps.split(",");
                    return Tuple3.of(name, Integer.parseInt(typeInfo[0]),Integer.parseInt(typeInfo[1]));
                }else {
                    return Tuple3.of(name, Integer.parseInt(ps),0);
                }
            }else {
                return Tuple3.of(name, 0, 0);
            }
        }else {
            return Tuple3.of(type, 0, 0);
        }
    }
}
