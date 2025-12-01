package com.dtstack.chunjun.connector.doris.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

public class RetryableStreamLoadWriter implements BufferFlusher {
    private static final Logger LOG = LoggerFactory.getLogger(RetryableStreamLoadWriter.class);
    private final int retries;
    private final StreamLoadWriter streamLoadWriter;

    public RetryableStreamLoadWriter(StreamLoadWriter streamLoadWriter, int retries) {
        this.retries = retries;
        this.streamLoadWriter = streamLoadWriter;
    }

    @Override
    public void write(InputStream inputStream, int length) throws Exception {
        streamLoadWriter.write(inputStream, length);
    }

    @Override
    public void write(Supplier<InputStream> supplier, int length) throws Exception {
        for (int retry = 1; retry <= retries; retry++) {
            try (InputStream inputStream = supplier.get()) {
                streamLoadWriter.write(inputStream, length);
                break;
            } catch (Exception e) {
                LOG.warn("Stream load failed on attempt {}. Error: {}", retry, e.getMessage());
                if (retry == retries) {
                    LOG.error("All {} retries failed. Throwing exception.", retries);
                    throw e;
                } else {
                    Thread.sleep(5000L);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        streamLoadWriter.close();
    }
}
