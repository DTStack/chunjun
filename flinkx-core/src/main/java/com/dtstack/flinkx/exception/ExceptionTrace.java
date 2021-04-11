package com.dtstack.flinkx.exception;

import java.util.Objects;

/**
 * @author tiezhu
 * @date 2021/2/2 星期二
 * Company dtstack
 */
public class ExceptionTrace {
    // 追溯当前异常的最原始异常信息
    public static String traceOriginalCause(Throwable e) {
        String errorMsg;
        if (Objects.nonNull(e.getCause())) {
            errorMsg = traceOriginalCause(e.getCause());
        } else {
            errorMsg = e.getMessage();
        }
        return errorMsg;
    }
}
