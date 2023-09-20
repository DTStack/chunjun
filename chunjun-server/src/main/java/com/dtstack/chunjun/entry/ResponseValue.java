package com.dtstack.chunjun.entry;

/**
 * web 返回的value
 * Company: www.dtstack.com
 * @author xuchao
 * @date 2023-09-19
 */
public class ResponseValue {

    /**返回状态码 0正常，1 异常*/
    private int code;
    /**返回结果*/
    private String data;
    /**状态为异常情况下返回的错误信息*/
    private String errorMsg;
    /**请求执行耗时*/
    private long space;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public long getSpace() {
        return space;
    }

    public void setSpace(long space) {
        this.space = space;
    }
}


