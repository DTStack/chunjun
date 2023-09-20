package com.dtstack.chunjun.config;

/**
 * session 对应yarn 的配置信息 Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-05-22
 */
public class YarnAppConfig {

    private String applicationName;

    private String queue;

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
}
