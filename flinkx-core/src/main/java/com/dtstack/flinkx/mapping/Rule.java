package com.dtstack.flinkx.mapping;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/14 星期二
 */
public class Rule {

    protected String sourceName;

    protected String sinkName;

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getSinkName() {
        return sinkName;
    }

    public void setSinkName(String sinkName) {
        this.sinkName = sinkName;
    }
}
