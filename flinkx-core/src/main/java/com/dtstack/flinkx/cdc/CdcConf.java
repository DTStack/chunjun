package com.dtstack.flinkx.cdc;

import com.dtstack.flinkx.cdc.store.FetcherConf;
import com.dtstack.flinkx.cdc.store.StoreConf;

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

    private StoreConf store;

    private FetcherConf fetcher;

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

    public void setSkipDDL(Boolean skipDDL) {
        this.skipDDL = skipDDL;
    }

    public StoreConf getStore() {
        return store;
    }

    public void setStore(StoreConf store) {
        this.store = store;
    }

    public FetcherConf getFetcher() {
        return fetcher;
    }

    public void setFetcher(FetcherConf fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CdcConf.class.getSimpleName() + "[", "]")
                .add("skipDDL=" + skipDDL)
                .add("workerNum=" + workerNum)
                .add("workerSize=" + workerSize)
                .add("workerMax=" + workerMax)
                .add("store=" + store)
                .add("fetcher=" + fetcher)
                .toString();
    }
}
