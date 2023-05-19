package com.dtstack.chunjun.server;

/**
 * session 当前的状态
 * Company: www.dtstack.com
 * @author xuchao
 * @date 2023-05-17
 */
public enum EStatus {
    RUNNING("RUNNING"),
    STOP("STOP");

    private final String value;

    private EStatus(String value){
        this.value = value;
    }

    public String getValue(){
        return this.getValue();
    }
}
