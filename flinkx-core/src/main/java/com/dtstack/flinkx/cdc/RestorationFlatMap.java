package com.dtstack.flinkx.cdc;

import com.dtstack.flinkx.cdc.store.Fetcher;
import com.dtstack.flinkx.cdc.store.Monitor;
import com.dtstack.flinkx.cdc.store.Store;
import com.dtstack.flinkx.cdc.worker.WorkerManager;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/1 星期三
 *     <p>数据（不论ddl还是dml数据）下发到对应表名下的unblock队列中，worker在轮询过程中，
 *     处理unblock数据队列中的数据，在遇到ddl数据之后，将数据队列置为block状态，并将队
 *     列引用交给store处理，store在拿到队列引用之后，将队列头部的ddl数据下发到外部存储中, 并监听外部存储对ddl的反馈情况（监听工作由store中额外的线程来执行），
 *     此时，队列仍然处于block状态；在收到外部存储的反馈之后，将数据队列头部的ddl数据移除，同时将队列状 态回归为unblock状态，队列引用还给worker。
 */
public class RestorationFlatMap implements FlatMapFunction<RowData, RowData> {

    private final ConcurrentHashMap<String, Deque<RowData>> blockedQueues =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Deque<RowData>> unblockQueues =
            new ConcurrentHashMap<>();

    private final Monitor monitor;

    private final WorkerManager workerManager;

    public RestorationFlatMap(Fetcher fetcher, Store store) {
        this.monitor = new Monitor(fetcher, store, blockedQueues, unblockQueues);
        this.workerManager = new WorkerManager(unblockQueues, blockedQueues);
    }

    @Override
    public void flatMap(RowData value, Collector<RowData> out) throws Exception {
        // TODD by tiezhu: 下发数据到unblock队列
        workerManager.work(out);
    }
}
