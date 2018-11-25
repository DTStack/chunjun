package com.dtstack.flinkx.carbondata;


import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Field;
import java.util.Map;

import java.io.IOException;

public class CarbondataUtil {

    private CarbondataUtil() {
        // hehe
    }

    private static SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();

    public static CarbonTable buildCarbonTable(String dbName, String tableName, String tablePath) throws IOException {
        String tableMetadataFile = CarbonTablePath.getSchemaFilePath(tablePath);
        org.apache.carbondata.format.TableInfo tableInfo = CarbonUtil.readSchemaFile(tableMetadataFile);
        TableInfo wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, tablePath);
        return CarbonTable.buildFromTableInfo(wrapperTableInfo);
    }

    public static void initFileFactory(Map<String,String> hadoopConfig) {
        Configuration conf = new Configuration();
        conf.clear();
        if(hadoopConfig != null) {
            for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }
        }
        conf.set("fs.hdfs.impl.disable.cache", "true");

        try {
            Field confField = FileFactory.class.getDeclaredField("configuration");
            confField.setAccessible(true);
            confField.set(null, conf);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }

}
