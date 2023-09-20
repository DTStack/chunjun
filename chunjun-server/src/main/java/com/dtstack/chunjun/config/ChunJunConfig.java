package com.dtstack.chunjun.config;

/**
 * chunjun server 配置 Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-06-13
 */
public class ChunJunConfig {

    private static final String DEFAULT_APP_NAME = "CHUNJUN-SESSION";

    private static final String DEFAULT_QUEUE = "default";

    /** 是否开启session check */
    private boolean enableSessionCheck = false;

    /** 是否开启自动部署session */
    private boolean enableSessionDeploy = false;

    /** 是否开启restful 服务，用于进行任务提交，日志查询，状态等查询*/
    private boolean enableRestful = false;

    private String flinkConfDir;

    private String flinkLibDir;

    private String flinkPluginLibDir;

    private String chunJunLibDir;

    private String hadoopConfDir;

    private String applicationName = DEFAULT_APP_NAME;

    private String queue = DEFAULT_QUEUE;

    private String chunJunLogConfDir;

    /** 默认的日志级别 */
    private String logLevel = "INFO";

    public boolean isEnableSessionCheck() {
        return enableSessionCheck;
    }

    public void setEnableSessionCheck(boolean enableSessionCheck) {
        this.enableSessionCheck = enableSessionCheck;
    }

    public String getFlinkConfDir() {
        return flinkConfDir;
    }

    public void setFlinkConfDir(String flinkConfDir) {
        this.flinkConfDir = flinkConfDir;
    }

    public String getHadoopConfDir() {
        return hadoopConfDir;
    }

    public void setHadoopConfDir(String hadoopConfDir) {
        this.hadoopConfDir = hadoopConfDir;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public boolean isEnableSessionDeploy() {
        return enableSessionDeploy;
    }

    public void setEnableSessionDeploy(boolean enableSessionDeploy) {
        this.enableSessionDeploy = enableSessionDeploy;
    }

    public String getFlinkLibDir() {
        return flinkLibDir;
    }

    public void setFlinkLibDir(String flinkLibDir) {
        this.flinkLibDir = flinkLibDir;
    }

    public String getChunJunLibDir() {
        return chunJunLibDir;
    }

    public void setChunJunLibDir(String chunJunLibDir) {
        this.chunJunLibDir = chunJunLibDir;
    }

    public String getFlinkPluginLibDir() {
        return flinkPluginLibDir;
    }

    public void setFlinkPluginLibDir(String flinkPluginLibDir) {
        this.flinkPluginLibDir = flinkPluginLibDir;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getChunJunLogConfDir() {
        return chunJunLogConfDir;
    }

    public void setChunJunLogConfDir(String chunJunLogConfDir) {
        this.chunJunLogConfDir = chunJunLogConfDir;
    }

    public boolean isEnableRestful() {
        return enableRestful;
    }

    public void setEnableRestful(boolean enableRestful) {
        this.enableRestful = enableRestful;
    }
}
