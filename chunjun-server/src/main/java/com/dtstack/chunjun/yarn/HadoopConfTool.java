package com.dtstack.chunjun.yarn;

import com.dtstack.chunjun.constants.ConstantValue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * 解析获取hadoop 的配置 Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-05-22
 */
public class HadoopConfTool {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopConfTool.class);

    public static final String FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    public static final String FS_LOCAL_IMPL_DISABLE_CACHE = "fs.file.impl.disable.cache";

    public static void setFsHdfsImplDisableCache(Configuration conf) {
        conf.setBoolean(FS_HDFS_IMPL_DISABLE_CACHE, true);
    }

    public static Configuration loadConf(String yarnConfDir) {
        Configuration yarnConf = new Configuration(false);
        try {

            File dir = new File(yarnConfDir);
            if (dir.exists() && dir.isDirectory()) {

                File[] xmlFileList =
                        new File(yarnConfDir)
                                .listFiles(
                                        (dir1, name) ->
                                                name.endsWith(ConstantValue.FILE_SUFFIX_XML));

                if (xmlFileList != null) {
                    for (File xmlFile : xmlFileList) {
                        yarnConf.addResource(xmlFile.toURI().toURL());
                    }
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return yarnConf;
    }

    /** 调整默认值 ipc.client.fallback-to-simple-auth-allowed: true */
    public static void replaceDefaultParam(Configuration yarnConf, Map<String, Object> yarnMap) {
        yarnConf.setBoolean(
                CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, true);

        if (yarnMap == null) {
            return;
        }

        if (yarnMap.get(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS) != null) {
            yarnConf.set(
                    YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
                    (String) yarnMap.get(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS));
        } else {
            yarnConf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, 15000L);
        }

        if (yarnMap.get(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS) != null) {
            yarnConf.set(
                    YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
                    (String)
                            yarnMap.get(
                                    YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS));
        } else {
            yarnConf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 5000L);
        }

        LOG.info(
                "yarn.resourcemanager.connect.max-wait.ms:{} yarn.resourcemanager.connect.retry-interval.ms:{}",
                yarnConf.getLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, -1),
                yarnConf.getLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, -1));
    }
}
