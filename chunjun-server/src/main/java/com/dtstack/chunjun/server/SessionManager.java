package com.dtstack.chunjun.server;

import com.dtstack.chunjun.client.YarnSessionClient;
import com.dtstack.chunjun.config.SessionConfig;
import com.dtstack.chunjun.config.YarnAppConfig;
import com.dtstack.chunjun.factory.ChunJunThreadFactory;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.yarn.YarnClusterDescriptor;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * session 管理器,目标是对session 进行监控和自管理
 * Company: www.dtstack.com
 * @author xuchao
 * @date 2023-05-16
 */
public class SessionManager {

    private static final Logger LOG = LoggerFactory.getLogger(SessionManager.class);

    private static final int DEPLOY_CHECK_INTERVAL = 2 * 1000;

    private static final String FLINK_VERSION = "flink116";

    private String sessionAppName;

    private SessionConfig sessionConfig;

    private YarnAppConfig yarnAppConfig;
    private ExecutorService sessionMonitorExecutor;
    private ScheduledExecutorService sessionDeployScheduler;
    private YarnSessionClient yarnSessionClient;
    private final SessionStatusInfo sessionStatusInfo = new SessionStatusInfo();
    private SessionDeployer sessionDeployer;

    private ClusterSpecification clusterSpecification;

    public  SessionManager(SessionConfig sessionConfig) {
        this.sessionConfig = sessionConfig;
        this.yarnAppConfig = sessionConfig.getAppConfig();
        this.sessionAppName = yarnAppConfig.getApplicationName();

        sessionMonitorExecutor = Executors.newSingleThreadExecutor();
        sessionDeployScheduler =
                new ScheduledThreadPoolExecutor(
                        1,
                        new ChunJunThreadFactory(
                                "session_deploy_factory",
                                true,
                                (t, e) -> {
                                    LOG.error("session_deploy_factory occur error!", e);
                                }));
        LOG.info("session name:{}", sessionAppName);

        initYarnSessionClient();
    }

    public void initYarnSessionClient() {
        yarnSessionClient = new YarnSessionClient(sessionConfig);
        yarnSessionClient.open();

        if (yarnSessionClient.getClient() == null) {
            // 初始化的时候检查远程是否存在匹配的session
            sessionStatusInfo.setStatus(EStatus.UNHEALTHY);
        } else {
            sessionStatusInfo.setAppId(yarnSessionClient.getClient().getClusterId().toString());
        }
    }

    /** 开启session 监控 */
    public void startSessionCheck() {
        SessionStatusMonitor sessionStatusMonitor =
                new SessionStatusMonitor(yarnSessionClient, sessionStatusInfo);
        sessionMonitorExecutor.submit(sessionStatusMonitor);
    }

    public void stopSessionCheck() {
        if (sessionMonitorExecutor != null) {
            sessionMonitorExecutor.shutdown();
            LOG.info("stopSessionCheck stopped");
        }
    }

    public void startSessionDeploy() {

        sessionDeployer = new SessionDeployer(sessionConfig);
        ClusterSpecification.ClusterSpecificationBuilder builder =
                new ClusterSpecification.ClusterSpecificationBuilder();
        // TODO 根据配置的参数设定session jvm,tm 内存,slot max 大小限制（在global conf 里设置）

        clusterSpecification = builder.createClusterSpecification();

        sessionDeployScheduler.scheduleWithFixedDelay(
                () -> {
                    if (!EStatus.UNHEALTHY.equals(sessionStatusInfo.getStatus())) {
                        return;
                    }

                    LOG.warn("current session status is unhealthy, will deploy a new session.");
                    doDeploy();
                },
                DEPLOY_CHECK_INTERVAL,
                DEPLOY_CHECK_INTERVAL,
                TimeUnit.MILLISECONDS);
    }

    public void doDeploy() {

        try (YarnClusterDescriptor yarnSessionDescriptor =
                sessionDeployer.createYarnClusterDescriptor()) {
            ClusterClient<ApplicationId> clusterClient =
                    yarnSessionDescriptor
                            .deploySessionCluster(clusterSpecification)
                            .getClusterClient();
            LOG.info("start session with cluster id :" + clusterClient.getClusterId().toString());

            // 重新开始session check
            sessionStatusInfo.setStatus(EStatus.HEALTHY);
        } catch (Throwable e) {
            LOG.error("Couldn't deploy Yarn session cluster, ", e);
            throw new RuntimeException(e);
        }
    }

    public void stopSessionDeploy() {
        if (sessionDeployScheduler != null) {
            sessionDeployScheduler.shutdown();
            LOG.info("sessionDeployScheduler stopped");
        }
    }

    public YarnSessionClient getYarnSessionClient() {
        return yarnSessionClient;
    }
}
