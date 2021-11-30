package com.dtstack.flinkx.cdc;

import java.io.Serializable;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/1 星期三
 *     <p>数据还原 cdc-restore 配置参数
 */
public class CdcRestoreConf implements Serializable {

    private static final long serialVersionUID = 1L;

    /** whether skip ddl statement or not. */
    private boolean isSkip;

    public boolean isSkip() {
        return isSkip;
    }

    public void setSkip(boolean skip) {
        isSkip = skip;
    }
}
