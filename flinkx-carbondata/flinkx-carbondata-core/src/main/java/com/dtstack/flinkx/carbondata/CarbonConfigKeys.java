package com.dtstack.flinkx.carbondata;

/**
 * This class defines configuration keys for CarbondataReader and CarbondataWriter
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbonConfigKeys {

    public static final String KEY_TABLE = "table";

    public static final String KEY_DATABASE = "database";

    public static final String KEY_HADOOP_CONFIG = "hadoopConfig";

    public static final String KEY_TABLE_PATH = "path";

    public static final String KEY_COLUMN_NAME = "name";

    public static final String KEY_COLUMN_TYPE = "type";

    public static final String KEY_COLUMN_VALUE = "value";

    public static final String KEY_FILTER = "filter";

    public static final String KEY_BATCH_SIZE = "batchSize";

    public static final int DEFAULT_BATCH_SIZE = 1024;




}
