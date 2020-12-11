package com.dtstack.flinkx.metadatapostgresql.constants;

import com.dtstack.flinkx.metadata.MetaDataCons;

/**
 * flinkx-all com.dtstack.flinkx.metadatapostgresql.constants


 * @description //常量定义
 * @author shitou
 * @date 2020/12/9 15:26
 */
public class PostgresqlCons extends MetaDataCons {
    /**
     驱动
    **/
    public static final String DRIVER_NAME = "org.postgresql.Driver";

    /**
     表名
    **/
    public static final String KEY_TABLE_NAME = "tableName";

    /**
     * column
     */
    public static final String KEY_COLUMN = "column";


    /**
    schemaName
    **/
    public static final String KEY_SCHEMA_NAME = "schemaName";


    /**
    tableSchema
    **/
    public static final String KEY_TABLE_SCHEMA = "tableSchema";


    /**
     sql语句：查询数据库中所有的表名
    **/
    public static final String SQL_SHOW_TABLES = "SELECT tablename FROM pg_tables";


    /**
     sql语句：查询表中字段信息
    **/
    public static final String SQL_SHOW_TABLE_COLUMN =
            "SELECT a.attname AS name,t.typname AS type, a.attlen AS length, a.atttypmod AS lengthvar \n" +

            ", a.attnotnull AS notnull , b.description AS comment\n" +

            "FROM pg_class c, pg_attribute a LEFT JOIN  pg_description b ON a.attrelid = b.objoid\n" +

            "AND a.attnum = b.objsubid, pg_type t WHERE c.relname = '%s' AND a.attnum > 0\n" +

            "AND a.attrelid = c.oid AND a.atttypid = t.oid ORDER BY  a.attnum";


}
