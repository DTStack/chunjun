package com.dtstack.chunjun.server;

import com.dtstack.chunjun.config.ChunJunConfig;
import com.dtstack.chunjun.config.ChunJunServerOptions;
import com.dtstack.chunjun.config.SessionConfig;
import com.dtstack.chunjun.config.WebConfig;
import com.dtstack.chunjun.config.YarnAppConfig;
import com.dtstack.chunjun.restapi.WebServer;
import com.dtstack.chunjun.server.util.EnvUtil;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;

import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_LIB_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_PLUGINS_DIR;
import static org.apache.flink.yarn.configuration.YarnConfigOptions.FLINK_DIST_JAR;

/**
 * 启动 chunjun server 服务 Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-06-13
 */
public class ServerLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(ServerLauncher.class);

    private ChunJunConfig chunJunConfig;

    private SessionManager sessionManager;

    private WebServer webServer;

    private SessionConfig sessionConfig;

    public void startServer() throws Exception {
        sessionConfig =
                new SessionConfig(
                        chunJunConfig.getFlinkConfDir(),
                        chunJunConfig.getHadoopConfDir(),
                        chunJunConfig.getFlinkLibDir(),
                        chunJunConfig.getChunJunLibDir());

        YarnAppConfig yarnAppConfig = new YarnAppConfig();
        yarnAppConfig.setApplicationName(chunJunConfig.getApplicationName());
        yarnAppConfig.setQueue(chunJunConfig.getQueue());

        sessionConfig.setAppConfig(yarnAppConfig);
        sessionConfig.loadFlinkConfiguration();
        sessionConfig.loadHadoopConfiguration();

        initEnv();
        sessionManager = new SessionManager(sessionConfig);
        // 是否启动session check 服务
        if (chunJunConfig.isEnableSessionCheck()) {
            LOG.info("open session check!");
            sessionManager.startSessionCheck();
        }

        // 是否开启session deploy 服务
        if (chunJunConfig.isEnableSessionDeploy()) {
            LOG.info("open session deploy!");
            sessionManager.startSessionDeploy();
        }

        // 启动restapi 服务
        if(chunJunConfig.isEnableRestful()){
            LOG.info("start web server ");
            WebConfig webConfig = new WebConfig();
            webConfig.setPort(18081);
            webServer = new WebServer(webConfig);
            webServer.startServer();
        }

        //注册shutdown hook
        Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));
    }

    public void stop(){

    }

    public ChunJunConfig getChunJunConfig() {
        return chunJunConfig;
    }

    public void setChunJunConfig(ChunJunConfig chunJunConfig) {
        this.chunJunConfig = chunJunConfig;
    }

    public void initEnv() throws Exception {
        // 如果环境变量没有设置ENV_FLINK_LIB_DIR, 但是进程要求必须传对应的参数，所以可以通过进程来设置
        // upload and register ship-only files，Plugin files only need to be shipped and should not
        // be added to classpath.
        Configuration configuration = sessionConfig.getFlinkConfig();

        if (System.getenv().get(ENV_FLINK_LIB_DIR) == null) {
            LOG.warn(
                    "Environment variable '{}' not set and ship files have not been provided manually.",
                    ENV_FLINK_LIB_DIR);
            Map<String, String> envMap = new HashMap<>();
            envMap.put(ENV_FLINK_LIB_DIR, sessionConfig.getFlinkLibDir());
            EnvUtil.setEnv(envMap);
        }

        //添加ChunJun dist 目录到configuration
        String chunJunLibDir = sessionConfig.getChunJunLibDir();
        if (chunJunLibDir != null) {
            chunJunLibDir = chunJunLibDir.startsWith("file:") ? chunJunLibDir : "file:" + chunJunLibDir;
            configuration.set(ChunJunServerOptions.CHUNJUN_DIST_PATH, chunJunLibDir);
        }

        // 添加对flinkplugin 的环境变量的设置
        String envFlinkPluginDir = System.getenv().get(ConfigConstants.ENV_FLINK_PLUGINS_DIR);
        if (envFlinkPluginDir == null) {
            Map<String, String> envMap = new HashMap<>();
            envMap.put(ConfigConstants.ENV_FLINK_PLUGINS_DIR, chunJunConfig.getFlinkPluginLibDir());
            EnvUtil.setEnv(envMap);
        }

        // 添加日志路径环境变量
        String logConfigPath = chunJunConfig.getChunJunLogConfDir();
        String levelPath =
                logConfigPath + File.separatorChar + chunJunConfig.getLogLevel().toLowerCase();

        Preconditions.checkArgument(new File(logConfigPath).exists(), levelPath+ " is not exists. please check log conf dir path.");

        sessionConfig
                .getFlinkConfig()
                .set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, levelPath);

        if (configuration.get(FLINK_DIST_JAR) == null) {
            // 设置yarnClusterDescriptor yarn.flink-dist-jar
            File flinkLibDir = new File(sessionConfig.getFlinkLibDir());
            if (!flinkLibDir.isDirectory()) {
                throw new RuntimeException(
                        String.format(
                                "param set flinkLibDir(%s) is not a dir, please check it.",
                                sessionConfig.getFlinkLibDir()));
            }
            Arrays.stream(flinkLibDir.listFiles())
                    .map(
                            file -> {
                                try {
                                    if (file.toURI().toURL().toString().contains("flink-dist")) {
                                        configuration.setString(
                                                FLINK_DIST_JAR, file.toURI().toURL().toString());
                                    }
                                } catch (MalformedURLException e) {
                                    LOG.warn("", e);
                                }

                                return file;
                            })
                    .collect(Collectors.toList());
        }
    }


    private static class ShutdownThread extends Thread{
        private ServerLauncher serverLauncher;

        public ShutdownThread(ServerLauncher serverLauncher){
            this.serverLauncher = serverLauncher;
        }

        public void run(){
            LOG.info("shutting down the chunjun serverLancher.");
            try{
                serverLauncher.stop();
            }catch (Exception e){
                LOG.error("failed to shut down the chunjun server lancher,", e);
            }

            LOG.info("chunjun serverLancher has been shutdown.");
        }
    }

    public static void main(String[] args) throws Exception {
        ServerLauncher serverLauncher = new ServerLauncher();

        // todo 通过配置文件加载,同时对必填参数进行检查
        ChunJunConfig chunJunConfig = new ChunJunConfig();
        String flinkConfDir = "/Users/xuchao/conf/flinkconf/dev_conf_local/";
        // String hadoopConfDir = "/Users/xuchao/conf/hadoopconf/dev_hadoop_flink01";
        String hadoopConfDir = "/Users/xuchao/conf/hadoopconf/dev_hadoop_new_flinkx01";
        String flinkLibDir = "/Users/xuchao/MyPrograms/Flink/flink-1.16/lib";
        String flinkPluginDir = "/Users/xuchao/MyPrograms/Flink/flink-1.16/plugins";
        String chunJunLibDir = "/Users/xuchao/IdeaProjects/chunjun/chunjun-dist";
        //String chunJunLibDir = "/Users/xuchao/IdeaProjects/chunjun/lib";
        String chunJunLogConfDir = "/Users/xuchao/IdeaProjects/chunjun/conf/logconf";

        chunJunConfig.setEnableSessionCheck(true);
        chunJunConfig.setFlinkConfDir(flinkConfDir);
        chunJunConfig.setHadoopConfDir(hadoopConfDir);
        chunJunConfig.setFlinkLibDir(flinkLibDir);
        chunJunConfig.setChunJunLibDir(chunJunLibDir);
        chunJunConfig.setFlinkPluginLibDir(flinkPluginDir);
        chunJunConfig.setEnableSessionDeploy(true);
        chunJunConfig.setChunJunLogConfDir(chunJunLogConfDir);
        chunJunConfig.setEnableRestful(true);

        serverLauncher.setChunJunConfig(chunJunConfig);

        serverLauncher.startServer();
        LOG.info("start chunjun server success!");

        // add shutdown hook
    }
}
