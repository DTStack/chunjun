package com.dtstack.chunjun.connector.kafka.serialization.ticdc;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/20 星期一
 */
public class TicdcColumnTypeHelper {

    private TicdcColumnTypeHelper() {}

    public static String findType(int type) {
        switch (type) {
            case 1:
                return "TINYINT";
            case 2:
                return "SMALLINT";
            case 3:
                return "INT";
            case 4:
                return "FLOAT";
            case 5:
                return "DOUBLE";
            case 6:
                return "NULL";
            case 7:
                return "TIMESTAMP";
            case 8:
                return "BIGINT";
            case 9:
                return "MEDIUMINT";
            case 10:
            case 14:
                return "DATE";
            case 11:
                return "TIME";
            case 12:
                return "DATETIME";
            case 13:
                return "YEAR";
            case 15:
                return "VARCHAR";
            case 16:
                return "BIT";
            case 245:
                return "JSON";
            case 246:
                return "DECIMAL";
            case 247:
                return "ENUM";
            case 248:
                return "SET";
            case 249:
                return "TINYTEXT";
            case 250:
                return "MEDIUMTEXT";
            case 251:
                return "LONGTEXT";
            case 252:
                return "TEXT";
            case 253:
                return "VARBINARY";
            case 254:
                return "CHAR";
            default:
                throw new IllegalArgumentException("Can not transformer type: " + type);
        }
    }
}
