package com.dtstack.flinkx.s3.format;

import java.io.IOException;
import java.io.Reader;

/**
 * Implementation of a {@link RequestIndexer} that buffers {@link ActionRequest ActionRequests}
 * before re-sending them to the Elasticsearch cluster upon request.
 */
public class LineOffsetBufferedReader extends Reader {
    private Reader in;

    private char cb[];
    private int nChars, nextChar;
    private long offset;

    private boolean skipLF = false;

    private static int defaultCharBufferSize = 8192;
    private static int defaultExpectedLineLength = 80;

    public LineOffsetBufferedReader(Reader in, int sz) {
        this(in,sz,0L);
    }

    public LineOffsetBufferedReader(Reader in, int sz,long offset) {
        super(in);
        if (sz <= 0)
            throw new IllegalArgumentException("Buffer size <= 0");
        this.in = in;
        cb = new char[sz];
        nextChar = nChars =  0;
        this.offset = offset;
    }

    public LineOffsetBufferedReader(Reader in) {
        this(in, defaultCharBufferSize);
    }
    public LineOffsetBufferedReader(Reader in,long offset) {
        this(in, defaultCharBufferSize,offset);
    }



    private void ensureOpen() throws IOException {
        if (in == null)
            throw new IOException("Stream closed");
    }

    private void fill() throws IOException {
        int dst = 0;
        int n;
        do {
            n = in.read(cb, dst, cb.length - dst);
        } while (n == 0);
        if (n > 0) {
            nChars = dst + n;
            nextChar = dst;
        }
    }

    public int read() throws IOException {
        throw new IOException("reset() not supported");
    }


    public int read(char cbuf[], int off, int len) throws IOException {
        throw new IOException("read(char cbuf[], int off, int len)) not supported");
    }

    /**
     * Process the incoming element to produce multiple {@link ActionRequest ActionsRequests}.
     * The produced requests should be added to the provided {@link RequestIndexer}.
     *
     * @param element incoming element to process
     * @param ctx     runtime context containing information about the sink instance
     * @param indexer request indexer that {@code ActionRequest} should be added to
     */
    String readLine(boolean ignoreLF) throws IOException {
        StringBuilder s = null;
        int startChar;

        synchronized (lock) {
            ensureOpen();
            boolean omitLF = ignoreLF || skipLF;

            bufferLoop:
            for (; ; ) {

                if (nextChar >= nChars)
                    fill();
                if (nextChar >= nChars) { /* EOF */
                    if (s != null && s.length() > 0)
                        return s.toString();
                    else
                        return null;
                }
                boolean eol = false;
                char c = 0;
                int i;

                /* Skip a leftover '\n', if necessary */
                if (omitLF && (cb[nextChar] == '\n'))
                    nextChar++;
                skipLF = false;
                omitLF = false;

                charLoop:
                for (i = nextChar; i < nChars; i++) {
                    c = cb[i];
                    if ((c == '\n') || (c == '\r')) {
                        eol = true;
                        break charLoop;
                    }
                }

                startChar = nextChar;
                nextChar = i;

                if (eol) {
                    String str;
                    if (s == null) {
                        str = new String(cb, startChar, i - startChar);
                    } else {
                        s.append(cb, startChar, i - startChar);
                        str = s.toString();
                    }
                    nextChar++;
                    if (c == '\r') {
                        skipLF = true;
                    }
                    offset = offset + i - startChar + 1;
                    return str;
                }

                if (s == null)
                    s = new StringBuilder(defaultExpectedLineLength);
                s.append(cb, startChar, i - startChar);
                offset = offset + i - startChar;
            }
        }
    }

    public String readLine() throws IOException {
        return readLine(false);
    }


    public long getOffset() {
        return offset;
    }

    public void close() throws IOException {
        synchronized (lock) {
            if (in == null)
                return;
            try {
                in.close();
            } finally {
                in = null;
                cb = null;
            }
        }
    }
}
