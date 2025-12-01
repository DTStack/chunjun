package com.dtstack.chunjun.connector.doris.buffer;

public interface IBufferPool {
    int write(String data);

    void flush() throws Exception;

    void shutdown() throws Exception;
}
