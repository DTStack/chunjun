package com.dtstack.chunjun.client;

import com.dtstack.chunjun.entry.JobDescriptor;

import java.io.IOException;

/**
 * 定义操作远程的接口
 *
 * <p>Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-05-22
 */
public interface IClient {

    /** 关联远程信息，获取 */
    void open();

    /** 关闭连接 */
    void close() throws IOException;

    /**
     * 获取任务日志
     *
     * @return
     */
    String getJobLog(String jobId);

    /**
     * 获取任务状态
     *
     * @return
     */
    String getJobStatus(String jobId) throws Exception;

    /**
     * 获取任务统计信息
     *
     * @return
     */
    String getJobStatics(String jobId);

    /** 提交任务 */
    String submitJob(JobDescriptor jobDescriptor) throws Exception;

    /** 取消任务 */
    void cancelJob(String jobId);
}
