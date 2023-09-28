package com.dtstack.chunjun.entry;

/**
 * 返回的任务状态信息
 * Company: www.dtstack.com
 * @author xuchao
 * @date 2023-09-20
 */
public class StatusVO {
    private String jobId;

    private String jobStatus;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(String jobStatus) {
        this.jobStatus = jobStatus;
    }
}
