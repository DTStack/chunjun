package com.dtstack.flinkx.cdc.monitor;

import com.dtstack.flinkx.cdc.QueuesChamberlain;
import com.dtstack.flinkx.cdc.WrapCollector;
import com.dtstack.flinkx.cdc.exception.LogExceptionHandler;
import com.dtstack.flinkx.cdc.monitor.fetch.FetcherBase;
import com.dtstack.flinkx.cdc.monitor.store.StoreBase;
import com.dtstack.flinkx.cdc.utils.ExecutorUtils;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

/**
 * 主要做两件事：
 *
 * <p>(1) 将blockQueue中的数据，通过store下发到外部数据源；
 *
 * <p>(2) 通过fetcher获取外部数据源对ddl的反馈，并将对应的ddl数据从blockQueue中删除，把数据队列放到unblockQueue中
 *
 * @author tiezhu@dtstack.com
 * @since 2021/12/2 星期四
 */
public class Monitor implements Serializable {

    private final FetcherBase fetcher;

    private final StoreBase store;

    /** 用来存储已经下发的ddl table */
    private final CopyOnWriteArrayList<String> storedTableIdentifier = new CopyOnWriteArrayList<>();

    private transient ExecutorService fetcherExecutor;

    private transient ExecutorService storeExecutor;

    private final QueuesChamberlain queuesChamberlain;

    public Monitor(FetcherBase fetcher, StoreBase store, QueuesChamberlain queuesChamberlain) {
        this.fetcher = fetcher;
        this.store = store;
        this.queuesChamberlain = queuesChamberlain;
    }

    public void open() throws Exception {
        fetcher.setChamberlain(queuesChamberlain);
        fetcher.setStoredTableIdentifier(storedTableIdentifier);

        store.setChamberlain(queuesChamberlain);
        store.setStoredTableIdentifier(storedTableIdentifier);

        fetcher.open();
        store.open();
    }

    public void work() {
        submitFetcher();
        submitStore();
    }

    private void submitFetcher() {
        fetcherExecutor =
                ExecutorUtils.singleThreadExecutor(
                        "fetcher-pool-%d", false, new LogExceptionHandler());
        fetcherExecutor.execute(fetcher);
    }

    private void submitStore() {
        storeExecutor =
                ExecutorUtils.singleThreadExecutor(
                        "store-pool-%d", false, new LogExceptionHandler());
        storeExecutor.execute(store);
    }

    public WrapCollector<RowData> getCollector() {
        return this.store.getCollector();
    }

    public void setCollector(WrapCollector<RowData> collector) {
        this.store.setCollector(collector);
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
