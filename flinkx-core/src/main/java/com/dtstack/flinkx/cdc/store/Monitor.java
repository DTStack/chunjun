package com.dtstack.flinkx.cdc.store;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/2 星期四
 *     <p>主要做两件事： (1) 将blockQueue中的数据，通过store下发到外部数据源； (2)
 *     通过fetcher获取外部数据源对ddl的反馈，并将对应的ddl数据从blockQueue中删除，把数据队列放到unblockQueue中
 */
public class Monitor implements Serializable {

    private final Map<String, Deque<RowData>> blockQueue;

    private final Map<String, Deque<RowData>> unblockQueue;

    private final Fetcher fetcher;

    private final Store store;

    private transient ExecutorService fetcherExecutor;

    private transient ExecutorService storeExecutor;

    public Monitor(
            Fetcher fetcher,
            Store store,
            Map<String, Deque<RowData>> blockQueue,
            Map<String, Deque<RowData>> unblockQueue) {
        this.fetcher = fetcher;
        this.store = store;
        this.blockQueue = blockQueue;
        this.unblockQueue = unblockQueue;
    }

    public void open() {
        fetcher.open();
        store.open();
    }

    public void work() {
        submitFetcher();
        submitStore();
    }

    private void submitFetcher() {
        fetcher.setDeque(blockQueue, unblockQueue);
        fetcherExecutor = Executors.newSingleThreadExecutor();
        fetcherExecutor.submit(fetcher);
    }

    private void submitStore() {
        store.setBlockDeque(blockQueue);
        storeExecutor = Executors.newSingleThreadExecutor();
        storeExecutor.submit(store);
    }

    public void close() {

        if (fetcher != null) {
            fetcher.close();
        }

        if (store != null) {
            store.close();
        }

        if (fetcherExecutor != null && !fetcherExecutor.isShutdown()) {
            fetcherExecutor.shutdown();
        }

        if (storeExecutor != null && !storeExecutor.isShutdown()) {
            storeExecutor.shutdown();
        }
    }
}
