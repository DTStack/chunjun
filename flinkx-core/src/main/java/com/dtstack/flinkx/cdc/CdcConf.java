package com.dtstack.flinkx.cdc;

import java.io.Serializable;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/1 星期三
 *     <p>数据还原 cdc-restore 配置参数
 */
public class CdcConf implements Serializable {

    private static final long serialVersionUID = 1L;

    private String type;

    /** whether skip ddl statement or not. */
    private boolean skip = true;

    public boolean isSkip() {
        return skip;
    }

    public void setSkip(Boolean skip) {
        this.skip = skip;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "CdcConf{" + "type='" + type + '\'' + ", skip=" + skip + '}';
    }
}
