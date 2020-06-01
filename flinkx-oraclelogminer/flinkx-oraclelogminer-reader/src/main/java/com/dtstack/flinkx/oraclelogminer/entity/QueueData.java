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
    private long scn;
    private Map<String, Object> data;

    public QueueData(long lsn, Map<String, Object> data) {
        this.scn = lsn;
        this.data = data;
    }

    public long getScn() {
        return scn;
    }

    public Map<String, Object> getData() {
        return data;
    }

    @Override
    public String toString() {
        return "QueueData{" +
                "scn=" + scn +
                ", data=" + new Gson().toJson(data) +
                '}';
    }
}
