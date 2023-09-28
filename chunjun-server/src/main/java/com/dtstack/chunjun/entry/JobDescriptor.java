package com.dtstack.chunjun.entry;

/**
 * Company: www.dtstack.com
 * @author xuchao
 * @date 2023-09-21
 */
public class JobDescriptor {

    private String mode;

    private String jobType;

    private String jobName;

    /**
     * 提交的任务信息-json 格式
     */
    private String job;

    /**
     * 任务额外属性信息
     */
    private String confProp;

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

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }
}
