package com.dtstack.flinkx.metadataphoenix5.util;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;

import java.io.IOException;
import java.io.StringReader;

public class Phoenix5Util {

    /**
     * 通过指定类加载器获取helper
     * @param parentClassLoader
     * @return
     * @throws IOException
     * @throws CompileException
     */
    public static IPhoenix5Helper getHelper(ClassLoader parentClassLoader) throws IOException, CompileException {
        ClassBodyEvaluator cbe = new ClassBodyEvaluator();
        cbe.setParentClassLoader(parentClassLoader);
        cbe.setDefaultImports("com.dtstack.flinkx.util.ClassUtil",
                "com.dtstack.flinkx.util.TelnetUtil",
                "org.apache.commons.lang3.StringUtils",
                "org.apache.commons.lang3.tuple.Pair",
                "org.apache.flink.types.Row",
                "org.apache.hadoop.hbase.NoTagsKeyValue",
                "org.apache.hadoop.hbase.client.Result",
                "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                "org.apache.phoenix.compile.RowProjector",
                "org.apache.phoenix.compile.StatementContext",
                "org.apache.phoenix.jdbc.PhoenixEmbeddedDriver",
                "org.apache.phoenix.jdbc.PhoenixPreparedStatement",
                "org.apache.phoenix.jdbc.PhoenixResultSet",
                "org.apache.phoenix.query.KeyRange",
                "org.apache.phoenix.schema.tuple.ResultTuple",
                "org.apache.phoenix.schema.types.PBoolean",
                "org.apache.phoenix.schema.types.PChar",
                "org.apache.phoenix.schema.types.PDataType",
                "org.apache.phoenix.schema.types.PDate",
                "org.apache.phoenix.schema.types.PDecimal",
                "org.apache.phoenix.schema.types.PDouble",
                "org.apache.phoenix.schema.types.PFloat",
                "org.apache.phoenix.schema.types.PInteger",
                "org.apache.phoenix.schema.types.PLong",
                "org.apache.phoenix.schema.types.PSmallint",
                "org.apache.phoenix.schema.types.PTime",
                "org.apache.phoenix.schema.types.PTimestamp",
                "org.apache.phoenix.schema.types.PTinyint",
                "org.apache.phoenix.schema.types.PUnsignedDate",
                "org.apache.phoenix.schema.types.PUnsignedDouble",
                "org.apache.phoenix.schema.types.PUnsignedFloat",
                "org.apache.phoenix.schema.types.PUnsignedInt",
                "org.apache.phoenix.schema.types.PUnsignedLong",
                "org.apache.phoenix.schema.types.PUnsignedSmallint",
                "org.apache.phoenix.schema.types.PUnsignedTime",
                "org.apache.phoenix.schema.types.PUnsignedTimestamp",
                "org.apache.phoenix.schema.types.PUnsignedTinyint",
                "org.apache.phoenix.schema.types.PVarchar",
                "java.lang.reflect.Field",
                "java.sql.Connection",
                "java.sql.DriverManager",
                "java.sql.PreparedStatement",
                "java.sql.ResultSet",
                "java.sql.SQLException",
                "java.util.ArrayList",
                "java.util.Collections",
                "java.util.HashMap",
                "java.util.List",
                "java.util.Map",
                "java.util.NavigableSet",
                "java.util.Properties");
        cbe.setImplementedInterfaces(new Class[]{IPhoenix5Helper.class});
        StringReader sr = new StringReader(IPhoenix5Helper.CLASS_STR);
        return (IPhoenix5Helper) cbe.createInstance(sr);
    }
}
