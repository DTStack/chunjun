/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.ftp.source;

import java.io.IOException;
import java.io.InputStream;

public class FtpInputStreamProxy extends InputStream {

    private final InputStream input;
    private final long readLimit;
    private long currentReadBytes;

    public FtpInputStreamProxy(InputStream input, long readLimit) {
        this.input = input;
        this.readLimit = readLimit;
        this.currentReadBytes = 0;
    }

    @Override
    public int read() throws IOException {
        if (currentReadBytes + 1 > readLimit) {
            return -1;
        } else {
            int ch = input.read();
            currentReadBytes += 1;
            return ch;
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (currentReadBytes + len <= readLimit) {
            int ch = input.read(b, off, len);
            currentReadBytes += ch;
            return ch;
        } else {
            int maxRead = (int) (readLimit - currentReadBytes);
            if (maxRead > 0) {
                int ch = input.read(b, off, maxRead);
                currentReadBytes += ch;
                return ch;
            } else {
                return -1;
            }
        }
    }

    @Override
    public long skip(long n) throws IOException {
        return input.skip(n);
    }

    @Override
    public synchronized void reset() throws IOException {
        input.reset();
    }

    @Override
    public boolean markSupported() {
        return input.markSupported();
    }

    @Override
    public int available() throws IOException {
        if (currentReadBytes > readLimit) {
            return -1;
        }

        return input.available();
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        input.mark(readlimit);
    }

    public long getCurrentReadBytes() {
        return currentReadBytes;
    }
}
