package com.dtstack.flinkx.s3;

import java.io.Serializable;
import java.util.Objects;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class S3SimpleObject implements Serializable {

    private static final long serialVersionUID = -7199607264925678753L;

    private String key;
    private long contentLength;

    public S3SimpleObject() {
    }

    public S3SimpleObject(String key, long contentLength) {
        this.key = key;
        this.contentLength = contentLength;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getContentLength() {
        return contentLength;
    }

    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        S3SimpleObject that = (S3SimpleObject) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }
}
