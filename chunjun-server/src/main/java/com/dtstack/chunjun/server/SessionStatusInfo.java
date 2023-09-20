package com.dtstack.chunjun.server;

/**
 * 当前session 的状态信息 Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-05-17
 */
public class SessionStatusInfo {

    private String appId;

    private EStatus status = EStatus.UNINIT;

    public SessionStatusInfo() {}

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public EStatus getStatus() {
        return status;
    }

    public void setStatus(EStatus status) {
        this.status = status;
    }
}
