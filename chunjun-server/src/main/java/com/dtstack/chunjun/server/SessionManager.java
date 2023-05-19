package com.dtstack.chunjun.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * session 管理器，目标是对session 进行监控和自管理
 * Company: www.dtstack.com
 * @author xuchao
 * @date 2023-05-16
 */
public class SessionManager {

    private static final Logger LOG = LoggerFactory.getLogger(SessionManager.class);

    private boolean autoStart = false;

    private boolean sessionCheck = false;

    private ExecutorService sessionMonitorExecutor = Executors.newSingleThreadExecutor();

    /**
     * 开启session 监控
     */
    public void startSessionCheck(){
        if(!sessionCheck){
            LOG.info("session check is false");
        }

        SessionStatusMonitor sessionStatusMonitor = new SessionStatusMonitor();
        sessionMonitorExecutor.submit(sessionStatusMonitor);
    }

    public void
}
