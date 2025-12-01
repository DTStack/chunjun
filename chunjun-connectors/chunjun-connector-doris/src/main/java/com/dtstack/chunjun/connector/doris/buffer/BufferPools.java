package com.dtstack.chunjun.connector.doris.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/** 主缓冲池类 */
public class BufferPools implements IBufferPool {
    private static final Logger LOG = LoggerFactory.getLogger(BufferPools.class);

    private final List<BufferPool> bufferPools;
    private final BlockingQueue<BufferPool> availableBuffers;
    private volatile BufferPool currentBuffer;
    private final ExecutorService flushExecutor;
    private final ReentrantLock poolLock = new ReentrantLock();
    private final AtomicInteger writeCount = new AtomicInteger(0);
    private final Consumer<Throwable> errorHandler;
    private final int flusherSize;

    public BufferPools(
            int poolSize,
            int bufferCapacity,
            BufferFlusher flusher,
            Consumer<Throwable> errorHandler,
            boolean keepOrder,
            Consumer<Long> numWriterCount,
            Consumer<Long> bytesWriterCount) {
        this.bufferPools = new ArrayList<>(poolSize);
        this.availableBuffers = new ArrayBlockingQueue<>(poolSize);
        this.flusherSize = keepOrder ? 1 : poolSize;
        this.flushExecutor = Executors.newFixedThreadPool(flusherSize);
        this.errorHandler = errorHandler;

        // 初始化所有小缓冲池
        for (int i = 0; i < poolSize; i++) {
            BufferPool buffer =
                    new BufferPool(bufferCapacity, i, flusher, numWriterCount, bytesWriterCount);
            bufferPools.add(buffer);
            availableBuffers.offer(buffer);
        }

        LOG.info("BufferPool 初始化完成，包含 " + poolSize + " 个缓冲区");
    }

    /** 写入数据到缓冲池 */
    public int write(String data) {
        if (data == null || "".equalsIgnoreCase(data.trim())) {
            return 0;
        }

        poolLock.lock();
        try {
            // 如果当前没有活跃的缓冲区，选择一个新的
            if (currentBuffer == null || currentBuffer.getState() == BufferPool.BufferState.FULL) {
                selectNewBuffer();
            }

            int written = currentBuffer.write(data);
            if (written > 0) {
                writeCount.incrementAndGet();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "写入 "
                                    + written
                                    + " 字节到缓冲区 "
                                    + currentBuffer.getBufferId()
                                    + "，总写入次数："
                                    + writeCount.get());
                }

                // 如果当前缓冲区已满，异步刷写并选择新缓冲区
                if (currentBuffer.getState() == BufferPool.BufferState.FULL) {
                    scheduleFlush(currentBuffer);
                    currentBuffer = null; // 清空当前缓冲区引用
                }
                return written;
            } else {
                // 当前缓冲区已满，尝试选择新的缓冲区
                if (currentBuffer.getState() == BufferPool.BufferState.FULL) {
                    scheduleFlush(currentBuffer);
                    currentBuffer = null;
                    return write(data); // 递归调用重试
                }
                return -1;
            }
        } finally {
            poolLock.unlock();
        }
    }

    /** 选择新的可用缓冲区 */
    private boolean selectNewBuffer() {
        try {
            LOG.info("选择新的缓冲区...");
            BufferPool newBuffer = availableBuffers.take();
            if (newBuffer != null) {
                currentBuffer = newBuffer;
                LOG.info(
                        "选择缓冲区 {} 作为当前写入目标，当前空闲缓冲区数量：{}",
                        newBuffer.getBufferId(),
                        availableBuffers.size());
                return true;
            }
            return false;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /** 安排异步刷写 */
    private void scheduleFlush(BufferPool buffer) {
        flushExecutor.submit(
                () -> {
                    try {
                        buffer.flush();
                        availableBuffers.offer(buffer);
                        LOG.info("缓冲区 " + buffer.getBufferId() + " 刷写完成并重新进入候选状态");
                    } catch (Exception e) {
                        LOG.error("刷写缓冲区 " + buffer.getBufferId() + " 时发生错误：" + e.getMessage(), e);
                        // 即使出错也要重置缓冲区
                        buffer.reset();
                        availableBuffers.offer(buffer);
                        errorHandler.accept(e);
                    }
                });
    }

    /** 主动刷写当前缓冲区 */
    @Override
    public void flush() {
        poolLock.lock();
        try {
            if (currentBuffer != null && currentBuffer.getPosition() > 1) {
                LOG.info("主动刷写当前缓冲区 {}", currentBuffer.getBufferId());
                scheduleFlush(currentBuffer);
                currentBuffer = null;
            } else {
                LOG.debug("当前缓冲区为空或无数据，跳过刷写");
            }
        } finally {
            poolLock.unlock();
        }
    }

    /** 获取缓冲池状态信息 */
    public void printStatus() {
        LOG.info("\n=== BufferPool 状态 ===");
        LOG.info("可用缓冲区数量：" + availableBuffers.size());
        LOG.info("当前活跃缓冲区：" + (currentBuffer != null ? currentBuffer.getBufferId() : "无"));
        LOG.info("总写入次数：" + writeCount.get());

        for (BufferPool buffer : bufferPools) {
            LOG.info(
                    "缓冲区 "
                            + buffer.getBufferId()
                            + " - 状态："
                            + buffer.getState()
                            + "，位置："
                            + buffer.getPosition()
                            + "/"
                            + buffer.getCapacity());
        }
        LOG.info("========================\n");
    }

    /** 关闭缓冲池 */
    public void shutdown() {
        LOG.info("刷写...");
        scheduleFlush(currentBuffer);
        LOG.info("正在关闭 BufferPool...");
        flushExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            flushExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        currentBuffer = null;
        for (BufferPool bufferPool : bufferPools) {
            try {
                bufferPool.shutdown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        bufferPools.clear();
        availableBuffers.clear();
        LOG.info("BufferPool 已关闭");
    }
}
