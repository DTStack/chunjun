package com.dtstack.chunjun.cdc;

import com.dtstack.chunjun.cdc.monitor.MonitorConf;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 * 数据还原 cdc-restore 配置参数
 *
 * @author tiezhu@dtstack.com
 * @since 2021/12/1 星期三
 */
public class CdcConf implements Serializable {

    private static final long serialVersionUID = 1L;

    /** whether skip ddl statement or not. */
    private boolean skipDDL = true;

    /** worker的核心线程数 */
    private int workerNum = 2;

    /** worker遍历队列时的步长 */
    private int workerSize = 3;

    /** worker线程池的最大容量 */
    private int workerMax = 3;

    private MonitorConf monitor;

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

    public boolean isSkipDDL() {
        return skipDDL;
    }

    public void setSkipDDL(boolean skipDDL) {
        this.skipDDL = skipDDL;
    }

    public MonitorConf getMonitor() {
        return monitor;
    }

    public void setMonitor(MonitorConf monitor) {
        this.monitor = monitor;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CdcConf.class.getSimpleName() + "[", "]")
                .add("skipDDL=" + skipDDL)
                .add("workerNum=" + workerNum)
                .add("workerSize=" + workerSize)
                .add("workerMax=" + workerMax)
                .add("monitor=" + monitor)
                .toString();
    }
}
