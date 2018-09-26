package com.dtstack.flinkx.launcher;

import org.apache.flink.configuration.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import java.io.File;
import java.io.FilenameFilter;

/**
 * Created by sishu.yss on 2018/9/26.
 */
public class FlinkUtil {

    public static Configuration getFlinkConfiguration(String flinkConfDir) {
        Configuration config = GlobalConfiguration.loadConfiguration(flinkConfDir);
        return config;
    }


    public static YarnConfiguration getYarnConfiguration(Configuration config, String yarnConfDir) throws Exception {
        YarnConfiguration yarnConf = new YarnConfiguration();
        config.setString(ConfigConstants.PATH_HADOOP_CONFIG, yarnConfDir);
        FileSystem.initialize(config);
        File dir = new File(yarnConfDir);
        if (dir.exists() && dir.isDirectory()) {
            File[] xmlFileList = new File(yarnConfDir).listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.endsWith(".xml")) {
                        return true;
                    }
                    return false;
                }
            });
            if (xmlFileList != null) {
                for (File xmlFile : xmlFileList) {
                    yarnConf.addResource(xmlFile.toURI().toURL());
                }
            }
        }
        return yarnConf;
    }

}
