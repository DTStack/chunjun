package com.dtstack.flinkx.launcher;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptorV2;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by sishu.yss on 2018/9/26.
 */
public class FlinkUtil {

    public static Configuration getFlinkConfiguration(String flinkConfDir) {
        Configuration config = GlobalConfiguration.loadConfiguration(flinkConfDir);
        return config;
    }


    public static org.apache.hadoop.conf.Configuration getYarnConfiguration(Configuration config, String yarnConfDir) throws Exception {
        org.apache.hadoop.conf.Configuration yarnConf = new YarnConfiguration();
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

    public static ClusterSpecification createDefaultClusterSpecification(Configuration configuration,int priority) {
        final int numberTaskManagers = 1;

        // JobManager Memory
        final int jobManagerMemoryMB = configuration.getInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY);

        // Task Managers memory
        final int taskManagerMemoryMB = configuration.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY);

        int slotsPerTaskManager = configuration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);

        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobManagerMemoryMB)
                .setTaskManagerMemoryMB(taskManagerMemoryMB)
                .setNumberTaskManagers(numberTaskManagers)
                .setSlotsPerTaskManager(slotsPerTaskManager)
                .setPriority(priority)
                .createClusterSpecification();
    }

    public static AbstractYarnClusterDescriptor createPerJobClusterDescriptor(Configuration flinkConfiguration, org.apache.hadoop.conf.Configuration yarnConf,String flinkJarPath,String queue) throws Exception {
        Configuration newConf = new Configuration(flinkConfiguration);
        newConf.setString(HighAvailabilityOptions.HA_CLUSTER_ID, UUID.randomUUID().toString());
        AbstractYarnClusterDescriptor clusterDescriptor = getClusterDescriptor(newConf,yarnConf, false);
        List<URL> classpaths = new ArrayList<URL>();
        if (StringUtils.isNotBlank(flinkJarPath)) {
            File[] jars = new File(flinkJarPath).listFiles();
            for (File file : jars){
                if (file.toURI().toURL().toString().contains("flink-dist")){
                    clusterDescriptor.setLocalJarPath(new Path(file.toURI().toURL().toString()));
                } else {
                    classpaths.add(file.toURI().toURL());
                }
            }
        } else {
            throw new RuntimeException("The Flink jar path is null");
        }
        clusterDescriptor.setProvidedUserJarFiles(classpaths);
        if(StringUtils.isNotBlank(queue)){
            clusterDescriptor.setQueue(queue);
        }
        return clusterDescriptor;
    }

    private static AbstractYarnClusterDescriptor getClusterDescriptor(Configuration flinkConfiguration, org.apache.hadoop.conf.Configuration yarnConf, boolean flip6) throws NoSuchFieldException, IllegalAccessException {
        AbstractYarnClusterDescriptor clusterDescriptor;
        if (flip6) {
            clusterDescriptor = new YarnClusterDescriptorV2(
                    flinkConfiguration,
                    ".");
        } else {
            clusterDescriptor = new YarnClusterDescriptor(
                    flinkConfiguration,
                    ".");
        }
        Field confField = AbstractYarnClusterDescriptor.class.getDeclaredField("conf");
        confField.setAccessible(true);
        confField.set(clusterDescriptor, yarnConf);
        return clusterDescriptor;
    }
}
