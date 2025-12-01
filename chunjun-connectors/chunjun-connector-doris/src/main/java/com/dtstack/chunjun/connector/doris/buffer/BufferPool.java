package com.dtstack.chunjun.connector.doris.buffer;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

class BufferPool implements IBufferPool {
    private static final Logger LOG = LoggerFactory.getLogger(BufferPool.class);
    private static final byte OPEN_BRACKET = 0x5B; // [
    private static final byte COMMA = 0x2C; // ,
    private static final byte CLOSE_BRACKET = 0x5D; // ]

    private final int capacity;
    private volatile int position;
    private volatile BufferState state;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final int bufferId;
    private final AtomicInteger writeCount = new AtomicInteger(0);
    private final BufferFlusher flusher;;
    private final Consumer<Long> numWriterCount;
    private final Consumer<Long> bytesWriterCount;

    private Slice slice;

    public enum BufferState {
        AVAILABLE, // 可用状态
        WRITING, // 正在写入
        FLUSHING, // 正在刷写
        FULL // 已满待刷写
    }

    public BufferPool(
            int capacity,
            int bufferId,
            BufferFlusher flusher,
            Consumer<Long> numWriterCount,
            Consumer<Long> bytesWriterCount) {
        this.capacity = capacity;
        this.slice = Slices.allocate(capacity);
        this.position = 0;
        this.bufferId = bufferId;
        this.flusher = flusher;
        this.numWriterCount = numWriterCount;
        this.bytesWriterCount = bytesWriterCount;
        this.reset();
    }

    /**
     * 写入数据到缓冲区
     *
     * @param data 要写入的数据
     * @return 实际写入的字节数，-1表示缓冲区已满
     */
    public int write(byte[] data) {
        writeLock.lock();
        try {
            return write(Slices.wrappedBuffer(data));
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 写入数据到缓冲区
     *
     * @param data 要写入的数据
     * @return 实际写入的字节数，-1表示缓冲区已满
     */
    public int write(String data) {
        writeLock.lock();
        try {
            return write(Slices.copiedBuffer(data, UTF_8));
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void flush() throws Exception {
        writeLock.lock();
        try {
            if (position > 1) {
                state = BufferState.FLUSHING;
                replaceLastCommonToClosingBracket();
                LOG.info(
                        "小缓冲区 {} 刷写数据，数据总量 {} MB，条数：{}",
                        bufferId,
                        (int) Math.ceil((double) position / (1024 * 1024)),
                        writeCount.get());
                numWriterCount.accept((long) writeCount.get());
                bytesWriterCount.accept((long) position);
                Slice output = slice.slice(0, position);
                LOG.info("开始异步刷写缓冲区 {}，数据大小：{} 字节", this.bufferId, position);
                // 执行实际的刷写操作
                flusher.write(output::getInput, position);
                LOG.info("缓冲区 {} 刷写完成，数据大小：{} 字节", this.bufferId, position);
                // 刷写完成后重置缓冲区并放回可用队列
                this.reset();
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void shutdown() throws Exception {
        this.flush();
        this.slice.clear();
        this.slice = null;
    }

    /**
     * 写入数据到缓冲区
     *
     * @param data 要写入的数据
     * @return 实际写入的字节数，-1表示缓冲区已满
     */
    public int write(Slice data) {
        writeLock.lock();
        try {
            if (state != BufferState.WRITING && state != BufferState.AVAILABLE) {
                return -1; // 缓冲区不可写
            }

            int availableSpace = capacity - position;
            if (availableSpace <= 0) {
                state = BufferState.FULL;
                return -1; // 缓冲区已满
            }

            if (data.length() + 1 + position > capacity) {
                state = BufferState.FULL;
                return -1; // 数据无法存入缓冲区
            }

            state = BufferState.WRITING;
            int bytesToWrite = data.length();
            slice.setBytes(position, data);
            position += bytesToWrite;
            slice.setByte(position, COMMA);
            position += 1;
            writeCount.incrementAndGet();

            if (position >= capacity) {
                state = BufferState.FULL;
            }

            return bytesToWrite;
        } finally {
            writeLock.unlock();
        }
    }

    // [{},{}, => [{},{}]
    private void replaceLastCommonToClosingBracket() {
        if (state == BufferState.FLUSHING) {
            slice.setByte(position - 1, CLOSE_BRACKET);
        }
    }

    /** 刷写完成后重置缓冲区 */
    public void reset() {
        writeLock.lock();
        try {
            slice.setByte(0, OPEN_BRACKET);
            position = 1;
            writeCount.set(0);
            state = BufferState.AVAILABLE;
            LOG.info("SmallBuffer " + bufferId + " 已重置为可用状态");
        } finally {
            writeLock.unlock();
        }
    }

    public BufferState getState() {
        return state;
    }

    public int getBufferId() {
        return bufferId;
    }

    public int getPosition() {
        return position;
    }

    public int getCapacity() {
        return capacity;
    }
}
