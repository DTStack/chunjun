package com.dtstack.flinkx.cdc.store;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/3 星期五
 */
public abstract class Store implements Runnable, Serializable {

    protected Map<String, Deque<RowData>> blockDeque;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public void run() {
        while (!closed.get()) {
            for (String table : blockDeque.keySet()) {
                // 1. 将block的ddl数据下发到外部数据源中
                Deque<RowData> rowData = blockDeque.get(table);
                RowData data = rowData.peekFirst();
                store(data);
            }
        }
    }

    public void setBlockDeque(Map<String, Deque<RowData>> blockDeque) {
        this.blockDeque = blockDeque;
    }

    public void close() {
        closed.compareAndSet(false, true);
        closeSubclass();
    }

    public abstract void store(RowData data);

    public abstract void open();

    public abstract void closeSubclass();
}
