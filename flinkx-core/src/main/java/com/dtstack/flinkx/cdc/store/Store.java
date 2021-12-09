package com.dtstack.flinkx.cdc.store;

import com.dtstack.flinkx.cdc.QueuesChamberlain;
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

    protected QueuesChamberlain chamberlain;

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

    public void setChamberlain(QueuesChamberlain chamberlain) {
        this.chamberlain = chamberlain;
    }

    public void close() {
        closed.compareAndSet(false, true);
        closeSubclass();
    }

    /**
     * 存储row data.
     *
     * @param data row data
     */
    public abstract void store(RowData data);

    /**
     * open sub-class
     *
     * @throws Exception exception
     */
    public abstract void open() throws Exception;

    /** class sub-class. */
    public abstract void closeSubclass();
}
