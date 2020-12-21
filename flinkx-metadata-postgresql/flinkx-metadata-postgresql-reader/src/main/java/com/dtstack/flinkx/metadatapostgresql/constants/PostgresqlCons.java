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
     databaseName
     */
    public static final String KEY_DATABASE_NAME = "databaseName";
    /**
     databaseSize
     */
    public static final String KEY_DATABASE_SIZE = "databaseSize";

    /**
     databaseOwner
     */
    public static final String KEY_DATABASE_OWNER = "databaseOwner";
    /**
     表名
    **/
    public static final String KEY_TABLE_NAME = "tableName";

    /**
     metaData
     */
    public static final String KEY_METADATA = "metaData";


    /**
     schemaName
    **/
    public static final String KEY_SCHEMA_NAME = "schemaName";


    /**
     sql语句：查询数据库中所有的表名，默认在schema为public的情况下
    **/
    public static final String SQL_SHOW_TABLES = "SELECT table_schema,table_name FROM information_schema.tables  WHERE table_schema = 'public'";

    /**
      sql语句：查询表中共有多少条数据（包含null值）
     */
    public static final String SQL_SHOW_COUNT = "SELECT count(*) AS count from %s";


    /**
     sql语句：查询表中字段信息
    **/
    public static final String SQL_SHOW_TABLE_COLUMN =
            "SELECT a.attname AS name,t.typname AS type, a.attlen AS length, a.atttypmod AS lengthvar \n" +

            ", a.attnotnull AS notnull , b.description AS comment\n" +

            "FROM pg_class c, pg_attribute a LEFT JOIN  pg_description b ON a.attrelid = b.objoid\n" +

            "AND a.attnum = b.objsubid, pg_type t WHERE c.relname = '%s' AND a.attnum > 0\n" +

            "AND a.attrelid = c.oid AND a.atttypid = t.oid ORDER BY  a.attnum";

    /**
      sql语句：查询表中的主键名
     */
    public static final String SQL_SHOW_TABLE_PRIMARYKEY =

            "SELECT pg_attribute.attname AS name FROM pg_index,pg_class,pg_attribute\n"+

            "WHERE pg_class.oid = '%s' :: regclass AND pg_index.indrelid = pg_class.oid\n"+

            "AND pg_attribute.attrelid = pg_class.oid AND pg_attribute.attnum = ANY (pg_index.indkey)";

    /**
     sql语句：查询表所占磁盘空间大小
     */
    public static final String SQL_SHOW_TABLE_SIZE =
            "SELECT pg_size_pretty(pg_total_relation_size('\"' || table_schema || '\".\"' || table_name || '\"')) AS size\n" +

            "FROM information_schema.tables WHERE table_name = '%s'";

    /**
      sql语句：查询数据库所占磁盘大小
     */
    public static final String SQL_SHOW_DATABASE_SIZE =
            "SELECT d.datname AS name,  pg_catalog.pg_get_userbyid(d.datdba) AS owner,\n" +

            "CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT')\n" +

            "THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname))\n" +

            "ELSE 'No Access' END AS size FROM pg_catalog.pg_database d WHERE d.datname = '%s'";

}
