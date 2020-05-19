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

package com.dtstack.flinkx.ftp.reader;

import com.dtstack.flinkx.ftp.IFtpHandler;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

/**
 * The InputStream Implementation that read multiple ftp files one by one.
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpSeqInputStream extends InputStream {
    private IFtpHandler ftpHandler;
    private Iterator<String> iter;
    private InputStream in;

    public FtpSeqInputStream(IFtpHandler ftpHandler, List<String> files) {
        this.ftpHandler = ftpHandler;
        this.iter = files.iterator();
        try {
            nextStream();
        } catch (IOException e) {
            // This should never happen
            throw new Error("panic");
        }
    }

    final void nextStream() throws IOException {
        if (in != null) {
            in.close();
        }

        if (iter.hasNext()) {
            String file = iter.next();
            in = ftpHandler.getInputStream(file);
            if (in == null) {
                throw new NullPointerException();
            }
        }
        else {
            in = null;
        }

    }

    @Override
    public int available() throws IOException {
        if (in == null) {
            // no way to signal EOF from available()
            return 0;
        }
        return in.available();
    }

    @Override
    public int read() throws IOException {
        while (in != null) {
            int c = in.read();
            if (c != -1) {
                return c;
            }
            nextStream();
        }
        return -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (in == null) {
            return -1;
        } else if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        do {
            int n = in.read(b, off, len);
            if (n > 0) {
                return n;
            }
            nextStream();
        } while (in != null);
        return -1;
    }

    @Override
    public void close() throws IOException {
        do {
            nextStream();
        } while (in != null);
    }


}
