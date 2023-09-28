package com.dtstack.chunjun.entry;

/**
 *
 *  提交任务运行的请求
 * Company: www.dtstack.com
 * @author xuchao
 * @date 2023-09-21
 */
public class JobSubmitReq {

    private String jobName;

    /**
     * 提交的任务信息-json 格式
     */
    private String job;

    /**
     * 任务额外属性信息
     */
    private String confProp;

    private boolean isURLDecode = false;

    public boolean isURLDecode() {
        return isURLDecode;
    }

    public void setURLDecode(boolean URLDecode) {
        isURLDecode = URLDecode;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getConfProp() {
        return confProp;
    }

    public void setConfProp(String confProp) {
        this.confProp = confProp;
    }
}
