package com.dtstack.chunjun.entry;

/**
 * 获取状态的请求
 * Company: www.dtstack.com
 * @author xuchao
 * @date 2023-09-20
 */
public class StatusReq extends BaseEntry{

    private String jobId;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }
}
