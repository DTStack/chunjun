package com.dtstack.flinkx.cdc.store;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/2 星期四
 *     <p>主要做两件事： (1) 将blockQueue中的数据，通过store下发到外部数据源； (2)
 *     通过fetcher获取外部数据源对ddl的反馈，并将对应的ddl数据从blockQueue中删除，把数据队列放到unblockQueue中
 */
public class Monitor implements Runnable, Serializable {

    private final Map<String, Deque<RowData>> blockQueue;

    private final Map<String, Deque<RowData>> unblockQueue;

    private final Fetcher fetcher;

    private final Store store;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private transient ExecutorService executor;

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
        executor = Executors.newSingleThreadExecutor();
        executor.submit(this);
    }

    @Override
    public void run() {
        while (!closed.get()) {
            // TODO by tiehzu: Fetcher的功能做为一个独立的线程来执行
            for (String table : blockQueue.keySet()) {
                // 1. 将block的ddl数据下发到外部数据源中
                Deque<RowData> rowData = blockQueue.get(table);
                RowData data = rowData.peekFirst();
                store.store(data);

                // 2. 轮训数据库，等待外部数据源的反馈
                if (fetcher.fetch(data)) {
                    // 如果外部数据源有所反馈，那么将blockQueue的头元素移除，并将数据队列放置在unblock中
                    rowData.removeFirst();
                    unblockQueue.put(table, rowData);
                    blockQueue.remove(table);
                }
            }
        }
    }

    public void close() {
        closed.compareAndSet(false, true);
        if (executor != null) {
            executor.shutdown();
        }
    }
}
