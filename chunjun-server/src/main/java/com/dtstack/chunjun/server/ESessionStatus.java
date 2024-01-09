package com.dtstack.chunjun.server;

/**
 * session 当前的状态 Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-05-17
 */
public enum ESessionStatus {
    // 未初始化状态,表明还为对接到具体的yarn app上
    UNINIT("UNINIT"),
    HEALTHY("HEALTHY"),
    UNHEALTHY("UNHEALTHY");

    private final String value;

    private ESessionStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.getValue();
    }
}
