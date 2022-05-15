package com.dtstack.chunjun.cdc.monitor.store;

import com.dtstack.chunjun.cdc.QueuesChamberlain;
import com.dtstack.chunjun.cdc.WrapCollector;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Deque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/3 星期五
 */
public abstract class StoreBase implements Runnable, Serializable {

    protected QueuesChamberlain chamberlain;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected CopyOnWriteArrayList<String> storedTableIdentifier;

    protected WrapCollector<RowData> collector;

    @Override
    public void run() {
        while (!closed.get()) {
            for (String table : chamberlain.blockTableIdentities()) {
                // 如果数据已经被下发了，那么就跳过
                if (storedTableIdentifier.contains(table)) {
                    continue;
                }
                // 将block的ddl数据下发到外部数据源中
                final Deque<RowData> rowDataDeque = chamberlain.fromBlock(table);
                RowData data = rowDataDeque.peekFirst();
                if (collector != null && store(data)) {
                    // ddl数据需要往下游发送 sink自身判断是否执行ddl语句
                    collector.collect(data);
                    storedTableIdentifier.add(table);
                }
            }
        }
    }

    public void setChamberlain(QueuesChamberlain chamberlain) {
        this.chamberlain = chamberlain;
    }

    public void setStoredTableIdentifier(CopyOnWriteArrayList<String> storedTableIdentifier) {
        this.storedTableIdentifier = storedTableIdentifier;
    }

    public void close() {
        closed.compareAndSet(false, true);
        closeSubclass();
    }

    public WrapCollector<RowData> getCollector() {
        return collector;
    }

    public void setCollector(WrapCollector<RowData> collector) {
        this.collector = collector;
    }

    /**
     * 存储row data.
     *
     * @param data row data
     * @return 是否存储成功
     */
    public abstract boolean store(RowData data);

    /**
     * open sub-class
     *
     * @throws Exception exception
     */
    public abstract void open() throws Exception;

    /** class sub-class. */
    public abstract void closeSubclass();
}
