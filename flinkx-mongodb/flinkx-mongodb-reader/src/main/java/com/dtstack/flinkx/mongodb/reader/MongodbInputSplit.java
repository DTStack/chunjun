package com.dtstack.flinkx.mongodb.reader;

import org.apache.flink.core.io.InputSplit;

/**
 * @author jiangbo
 * @date 2018/6/5 16:50
 */
public class MongodbInputSplit implements InputSplit {

    private int skip;

    private int limit;

    public MongodbInputSplit(int skip, int limit) {
        this.skip = skip;
        this.limit = limit;
    }

    public int getSkip() {
        return skip;
    }

    public void setSkip(int skip) {
        this.skip = skip;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public int getSplitNumber() {
        return 0;
    }
}
