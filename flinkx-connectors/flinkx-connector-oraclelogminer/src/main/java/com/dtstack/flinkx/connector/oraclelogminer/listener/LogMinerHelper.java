/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.connector.oraclelogminer.listener;

import com.dtstack.flinkx.connector.oraclelogminer.conf.LogMinerConf;
import com.dtstack.flinkx.connector.oraclelogminer.entity.QueueData;
import com.dtstack.flinkx.connector.oraclelogminer.util.SqlUtil;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LogMinerHelper {

    public static Logger LOG = LoggerFactory.getLogger(LogMinerHelper.class);

    private final TransactionManager transactionManager;
    private final ExecutorService connectionExecutor;
    /** 加载数据的connection */
    private final LinkedList<LogMinerConnection> activeConnectionList;

    private final LogMinerConf config;
    private final String logMinerSelectSql;
    private final LogMinerListener listener;
    private final BigInteger step = new BigInteger("3000");
    private BigInteger startScn;
    private BigInteger endScn;
    // 是否加载了online实时日志
    private Boolean loadRedo = false;

    // 最后一条数据的位点
    private BigInteger currentSinkPosition;
    /** 当前正在读取的connection索引 * */
    private int currentIndex;
    /** 当前正在读取的connection * */
    private LogMinerConnection currentConnection;
    /** 当前正在读取的connection的endScn * */
    private BigInteger currentReadEndScn;

    public LogMinerHelper(
            LogMinerListener listener, LogMinerConf logMinerConfig, BigInteger startScn) {
        this.listener = listener;
        this.transactionManager =
                new TransactionManager(
                        logMinerConfig.getTransactionCacheNumSize(),
                        logMinerConfig.getTransactionExpireTime());
        this.startScn = startScn;
        this.endScn = startScn;
        this.activeConnectionList = new LinkedList<>();
        this.config = logMinerConfig;
        this.currentIndex = 0;

        ThreadFactory namedThreadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("LogMinerConnection-pool-%d")
                        .setUncaughtExceptionHandler(
                                (t, e) -> LOG.warn("LogMinerConnection run failed", e))
                        .build();

        connectionExecutor =
                new ThreadPoolExecutor(
                        logMinerConfig.getParallelism(),
                        logMinerConfig.getParallelism() + 2,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1024),
                        namedThreadFactory,
                        new ThreadPoolExecutor.AbortPolicy());

        for (int i = 0; i < logMinerConfig.getParallelism(); i++) {
            LogMinerConnection logMinerConnection =
                    new LogMinerConnection(logMinerConfig, transactionManager);
            activeConnectionList.add(logMinerConnection);
            logMinerConnection.connect();
            activeConnectionList.get(0).checkPrivileges();
        }
        this.logMinerSelectSql =
                SqlUtil.buildSelectSql(
                        config.getCat(),
                        config.getListenerTables(),
                        activeConnectionList.get(0).oracleInfo.isCdbMode());
        currentConnection = null;
        currentReadEndScn = null;
    }

    /** 初始化加载 如果初始化时就出现问题就直接结束任务 */
    public void init() {
        try {
            preLoad();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public BigInteger getStartScn(BigInteger scn) {
        return activeConnectionList.get(0).getStartScn(scn);
    }

    /** 预先加载日志文件 初始化 或者 currentConnection 读取完，下一个connection不存在时 */
    private void preLoad() throws SQLException {

        BigInteger currentMaxScn = null;
        // 遍历获取可以加载数据的connection
        List<LogMinerConnection> needLoadList =
                activeConnectionList.stream()
                        .filter(
                                i ->
                                        i.getState().equals(LogMinerConnection.STATE.READEND)
                                                || i.getState()
                                                        .equals(
                                                                LogMinerConnection.STATE
                                                                        .INITIALIZE))
                        .collect(Collectors.toList());
        for (LogMinerConnection logMinerConnection : needLoadList) {
            logMinerConnection.checkAndResetConnection();
            if (Objects.isNull(currentMaxScn)) {
                currentMaxScn = logMinerConnection.getCurrentScn();
            }
            // currentReadEndScn为空（第一次加载 保证初始化时至少有一个线程加载日志文件）或者
            // 当前加载的日志范围比数据库最大SCN差距超过3000则再起一个connection进行加载
            if (Objects.isNull(currentConnection)
                    || currentMaxScn.subtract(this.endScn).compareTo(step) > 0) {

                // 按照加载日志文件大小限制，根据endScn作为起点找到对应的一组加载范围
                BigInteger currentStartScn = Objects.nonNull(this.endScn) ? this.endScn : startScn;

                // 如果加载了redo日志，则起点不能是上一次记载的日志的结束位点，而是上次消费的最后一条数据的位点
                if (loadRedo) {
                    // 需要加1  因为logminer查找数据是左闭右开，如果不加1  会导致最后一条数据重新消费
                    currentStartScn = currentSinkPosition.add(BigInteger.ONE);
                }

                Pair<BigInteger, Boolean> endScn =
                        logMinerConnection.getEndScn(currentStartScn, new ArrayList<>(32));
                logMinerConnection.startOrUpdateLogMiner(currentStartScn, endScn.getLeft());
                // 读取v$logmnr_contents 数据由线程池加载
                loadData(logMinerConnection, logMinerSelectSql);
                this.endScn = endScn.getLeft();
                this.loadRedo = endScn.getRight();
                if (Objects.isNull(currentConnection)) {
                    updateCurrentConnection(logMinerConnection);
                }
                // 如果已经加载了redoLog就不需要多线程加载了
                if (endScn.getRight()) {
                    break;
                }
            } else {
                break;
            }
        }

        // 如果当前currentConnection为空 且没有可加载的connection，则将第一个connection替换重新加载
        if (Objects.isNull(currentConnection) && CollectionUtils.isEmpty(needLoadList)) {
            LOG.info(
                    "reset activeConnectionList[0] a new connection,activeConnectionList is {}",
                    activeConnectionList);
            activeConnectionList.set(0, new LogMinerConnection(config, transactionManager));
            preLoad();
        }
        LOG.info(
                "current load scnRange startSCN:{}, endSCN:{},currentReadEndScn:{},activeConnectionList:{}",
                startScn,
                endScn,
                currentReadEndScn,
                activeConnectionList);
    }

    /** 交由线程池加载数据到视图 */
    public void loadData(LogMinerConnection logMinerConnection, String sql) {
        connectionExecutor.submit(
                () -> {
                    try {
                        logMinerConnection.queryData(sql);
                    } catch (Exception e) {
                        listener.sendException(e, null);
                        restart(logMinerConnection);
                    }
                });
    }

    /** 当前connection重新加载即可 */
    public void restart() {
        LogMinerConnection logMinerConnection = activeConnectionList.get(currentIndex);
        restart(logMinerConnection);
    }

    /** connection重新加载 */
    public void restart(LogMinerConnection connection) {
        LOG.info(
                "restart connection, startScn: {},endScn: {}",
                connection.startScn,
                connection.endScn);
        try {
            connection.disConnect();
            if (listener.getCurrentPosition().compareTo(connection.endScn) >= 0) {
                throw new RuntimeException(
                        "the SCN currently consumed ["
                                + listener.getCurrentPosition()
                                + "] is larger than the endScn of the restarted connection ["
                                + connection.endScn
                                + "]");
            }
            // 加载的区间是左闭右开 所以需要把 listener.getCurrentPosition() 加 1
            connection.startOrUpdateLogMiner(
                    connection.startScn.compareTo(listener.getCurrentPosition()) > 0
                            ? connection.startScn
                            : listener.getCurrentPosition().add(BigInteger.ONE),
                    connection.endScn);
            loadData(connection, logMinerSelectSql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean hasNext() throws UnsupportedEncodingException, SQLException, DecoderException {

        if (Objects.isNull(currentConnection)) {
            LogMinerConnection connection = chooseAndPreLoadConnection();
            if (Objects.isNull(connection)) {
                return false;
            }
        }

        LogMinerConnection.STATE state = currentConnection.getState();

        // 如果当前connection是在加载阶段 则返回false 直到加载完成
        if (currentConnection.isLoading()) {
            return false;
        } else if (state.equals(LogMinerConnection.STATE.FAILED)) {
            restart();
            return false;
        }

        boolean hasNext = currentConnection.hasNext();

        // 当前connection读取完毕 设置为null
        if (!hasNext) {
            currentConnection = null;
        }
        return hasNext;
    }

    /** 选择当前读取的connection并预加载后续日志数据 */
    private LogMinerConnection chooseAndPreLoadConnection() throws SQLException {
        LogMinerConnection connection = chooseConnection();

        if (Objects.isNull(connection)) {
            this.endScn = currentReadEndScn;
            preLoad();
            if (Objects.isNull(connection = chooseConnection())) {
                throw new RuntimeException(
                        "has not choose connection,currentReadEndScn:["
                                + currentReadEndScn
                                + "],and connections is \n"
                                + activeConnectionList);
            }
            updateCurrentConnection(connection);
        } else {
            updateCurrentConnection(connection);
            preLoad();
        }
        return connection;
    }

    /** 更新当前读取connection信息 */
    public void updateCurrentConnection(LogMinerConnection connection) {
        currentIndex = activeConnectionList.indexOf(connection);
        currentConnection = activeConnectionList.get(currentIndex);
        this.startScn = currentConnection.startScn;
        this.currentReadEndScn = currentConnection.endScn;
        LOG.info(
                "after update currentConnection,currentIndex is {}, startScnOfCurrentConnection:{}, endScnOfCurrentConnection:{}, this.startScn:{},this.endScn:{},this.currentReadEndScn:{}",
                currentIndex,
                currentConnection.startScn,
                currentConnection.endScn,
                this.startScn,
                this.endScn,
                this.currentReadEndScn);
    }

    public void stop() {
        if (null != connectionExecutor && !connectionExecutor.isShutdown()) {
            connectionExecutor.shutdown();
        }
        if (CollectionUtils.isNotEmpty(activeConnectionList)) {
            activeConnectionList.forEach(LogMinerConnection::disConnect);
        }
    }

    /** 找出connection的startScn和当前currentReadEndScn相等的connection */
    public LogMinerConnection chooseConnection() {
        if (Objects.nonNull(currentConnection)) {
            return currentConnection;
        }
        LogMinerConnection choosedConnection = null;
        List<LogMinerConnection> candidateList =
                activeConnectionList.stream()
                        .filter(i -> Objects.nonNull(i.startScn) && Objects.nonNull(i.endScn))
                        .collect(Collectors.toList());
        for (LogMinerConnection logMinerConnection : candidateList) {
            if (logMinerConnection.startScn.compareTo(currentReadEndScn) == 0
                    && !logMinerConnection.getState().equals(LogMinerConnection.STATE.INITIALIZE)) {
                choosedConnection = logMinerConnection;
                if (choosedConnection.getState().equals(LogMinerConnection.STATE.FAILED)) {
                    restart();
                }
                break;
            }
        }
        return choosedConnection;
    }

    public QueueData getQueueData() {
        QueueData next = activeConnectionList.get(currentIndex).next();
        if (BigInteger.ZERO.compareTo(next.getScn()) != 0) {
            this.currentSinkPosition = next.getScn();
        }
        return next;
    }

    public void setStartScn(BigInteger startScn) {
        this.startScn = startScn;
        this.currentSinkPosition = this.startScn;
    }
}
