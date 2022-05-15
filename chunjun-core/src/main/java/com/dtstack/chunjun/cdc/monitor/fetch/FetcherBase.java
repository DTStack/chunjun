package com.dtstack.chunjun.cdc.monitor.fetch;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.QueuesChamberlain;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/2 星期四
 */
public abstract class FetcherBase implements Runnable, Serializable {

    private QueuesChamberlain chamberlain;

    protected final AtomicBoolean closed = new AtomicBoolean(false);

    protected CopyOnWriteArrayList<String> storedTableIdentifier;

    public void setChamberlain(QueuesChamberlain chamberlain) {
        this.chamberlain = chamberlain;
    }

    public void setStoredTableIdentifier(CopyOnWriteArrayList<String> storedTableIdentifier) {
        this.storedTableIdentifier = storedTableIdentifier;
    }

    public void open() throws Exception {
        openSubclass();

        // 查询外部数据源中是否存有ddl的数据
        Map<String, DdlRowData> ddlRowDataMap = query();
        if (null != ddlRowDataMap) {
            ddlRowDataMap.forEach(
                    (tableIdentity, ddlData) -> chamberlain.block(tableIdentity, ddlData));
        }
    }

    @Override
    public void run() {
        while (!closed.get()) {
            // 遍历block数据队列里的数据
            for (String table : chamberlain.blockTableIdentities()) {
                // 取队列中的头节点，查询外部数据源
                final Deque<RowData> rowDataDeque = chamberlain.fromBlock(table);
                RowData rowData = rowDataDeque.peekFirst();
                // 如果外部数据源已经处理了该数据，那么将此数据从数据队列中移除，此数据队列从block中移除，放入到unblock队列中
                if (fetch(rowData)) {
                    rowDataDeque.removeFirst();
                    chamberlain.unblock(table, rowDataDeque);
                    storedTableIdentifier.remove(table);
                    delete(rowData);
                }
            }
        }
    }

    /**
     * 查询外部数据源，判断当前data是否被处理
     *
     * @param data 需要查询的外部数据源
     * @return 是否被外部数据源处理
     */
    public abstract boolean fetch(RowData data);

    /**
     * 删除外部数据源对应的ddl data
     *
     * @param data 需要删除的ddl data
     */
    public abstract void delete(RowData data);

    /**
     * 查询外部数据源中未被处理的ddl数据，返回map of tableIdentity, ddl-row-data。
     *
     * @return map of table-identities and ddl row-data.
     */
    public abstract Map<String, DdlRowData> query();

    /**
     * open sub-class
     *
     * @throws Exception exception
     */
    public abstract void openSubclass() throws Exception;

    /** 关闭子类 */
    public abstract void closeSubclass();

    public void close() {
        closeSubclass();
        closed.compareAndSet(false, true);
    }
}
