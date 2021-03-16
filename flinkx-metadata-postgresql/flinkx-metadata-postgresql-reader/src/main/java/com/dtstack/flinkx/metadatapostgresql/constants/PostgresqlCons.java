package com.dtstack.flinkx.metadatapostgresql.constants;

import com.dtstack.metadata.rdb.core.constants.RdbCons;

/**
 * 常量定义
 *
 * @author shitou
 * @date 2020/12/9 15:26
 */
public class PostgresqlCons extends RdbCons {

    /**驱动*/
    public static final String DRIVER_NAME = "org.postgresql.Driver";

    /**表名*/
    public static final String KEY_TABLE_NAME = "tableName";

    /**schemaName*/
    public static final String KEY_SCHEMA_NAME = "schemaName";

    /**设置搜索路径*/
    public static final String SQL_SET_SEARCHPATH = "set search_path = %s";

    /**sql语句：查询数据库中所有的表名*/
    public static final String SQL_SHOW_TABLES = "select table_schema,table_name from information_schema.tables where table_schema <> 'information_schema' and  table_schema <> 'pg_catalog'";

    /**sql语句：查询表中共有多少条数据（包含null值）*/
    public static final String SQL_SHOW_COUNT = "select count(1) as count from %s";

    /**查询索引名及创建索引的SQL语句*/
    public static final String SQL_SHOW_INDEX = "select schemaname,tablename,indexname,indexdef from pg_indexes where schemaname = '%s' and tablename = '%s'";

    /**sql语句：查询表所占磁盘空间大小*/
    public static final String SQL_SHOW_TABLE_SIZE = "select pg_relation_size('%s.%s') as size";

}
