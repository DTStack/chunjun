package com.dtstack.chunjun.connector.doris.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

public interface BufferFlusher {
    void write(InputStream inputStream, int length) throws Exception;

    void write(Supplier<InputStream> supplier, int length) throws Exception;

    void close() throws IOException;
}
