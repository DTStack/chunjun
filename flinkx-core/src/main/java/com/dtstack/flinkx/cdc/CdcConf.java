package com.dtstack.flinkx.cdc;

import java.io.Serializable;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/1 星期三
 *     <p>数据还原 cdc-restore 配置参数
 */
public class CdcConf implements Serializable {

    private static final long serialVersionUID = 1L;

    private String type;

    /** whether skip ddl statement or not. */
    private boolean skip = true;

    /**
     * worker的核心线程数
     */
    private int workerNum = 2;

    /**
     * worker遍历队列时的步长
     */
    private int workerSize = 3;

    /**
     * worker线程池的最大容量
     */
    private int workerMax = 3;


    public int getWorkerNum() {
        return workerNum;
    }

    public void setWorkerNum(int workerNum) {
        this.workerNum = workerNum;
    }

    public int getWorkerSize() {
        return workerSize;
    }

    public void setWorkerSize(int workerSize) {
        this.workerSize = workerSize;
    }

    public int getWorkerMax() {
        return workerMax;
    }

    public void setWorkerMax(int workerMax) {
        this.workerMax = workerMax;
    }

    public boolean isSkip() {
        return skip;
    }

    public void setSkip(Boolean skip) {
        this.skip = skip;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "CdcConf{" +
                "type='" + type + '\'' +
                ", skip=" + skip +
                ", workerNum=" + workerNum +
                ", workerSize=" + workerSize +
                ", workerMax=" + workerMax +
                '}';
    }
}
