package com.dtstack.flinkx.oraclelogminer.entity;

import com.google.gson.Gson;

import java.util.Map;

/**
 * Date: 2020/06/01
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class QueueData {
    private long lsn;
    private Map<String, Object> data;

    public QueueData(long lsn, Map<String, Object> data) {
        this.lsn = lsn;
        this.data = data;
    }

    public long getLsn() {
        return lsn;
    }

    public Map<String, Object> getData() {
        return data;
    }

    @Override
    public String toString() {
        return "QueueData{" +
                "lsn=" + lsn +
                ", data=" + new Gson().toJson(data) +
                '}';
    }
}
