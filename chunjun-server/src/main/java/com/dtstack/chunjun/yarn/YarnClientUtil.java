package com.dtstack.chunjun.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-06-27
 */
public class YarnClientUtil {

    private static final Logger LOG = LoggerFactory.getLogger(YarnClientUtil.class);

    public static YarnClient buildYarnClient(Configuration yarnConf) {
        long startTime = System.currentTimeMillis();
        YarnClient yarnClient;
        try {
            LOG.info("build yarn client.");
            yarnClient = YarnClient.createYarnClient();
            yarnClient.init(yarnConf);
            yarnClient.start();
            long endTime = System.currentTimeMillis();
            LOG.info("build yarn client success. cost {} ms.", endTime - startTime);
        } catch (Throwable e) {
            LOG.error("build yarn client error.", e);
            throw new RuntimeException(e);
        }

        return yarnClient;
    }
}
