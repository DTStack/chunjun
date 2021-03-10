package com.dtstack.flinkx.metatdata.hive.core.util;

import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.ORC_FORMAT;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.PARQUET_FORMAT;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.TEXT_FORMAT;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.TYPE_ORC;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.TYPE_PARQUET;
import static com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons.TYPE_TEXT;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-21 17:29
 * @Description:
 */
public class HiveOperatorUtils {

    public static String getStoredType(String storedClass) {
        if (storedClass.endsWith(TEXT_FORMAT)){
            return TYPE_TEXT;
        } else if (storedClass.endsWith(ORC_FORMAT)){
            return TYPE_ORC;
        } else if (storedClass.endsWith(PARQUET_FORMAT)){
            return TYPE_PARQUET;
        } else {
            return storedClass;
        }
    }
}
