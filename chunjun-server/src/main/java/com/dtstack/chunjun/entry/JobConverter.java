package com.dtstack.chunjun.entry;

import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-09-21
 */
public class JobConverter {

    public static String[] convertJobToArgs(JobDescriptor jobDescriptor)
            throws IllegalAccessException {
        List<String> argList = new ArrayList<>();
        Field[] fields = jobDescriptor.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String val = (String) field.get(jobDescriptor);
            argList.add("-" + field.getName());
            argList.add(val);
        }

        return argList.toArray(new String[argList.size()]);
    }

    /**
     * @param jobSubmitReq
     * @return
     */
    public static JobDescriptor convertReqToJobDescr(JobSubmitReq jobSubmitReq) {
        JobDescriptor jobDescriptor = new JobDescriptor();
        // 当前系统提供的server 模式只支持yarn session 模式下的数据同步场景
        jobDescriptor.setJobType("sync");
        jobDescriptor.setMode("yarn-session");

        String job =
                jobSubmitReq.isURLDecode()
                        ? jobSubmitReq.getJob()
                        : URLEncoder.encode(jobSubmitReq.getJob());
        jobDescriptor.setJob(job);
        jobDescriptor.setConfProp(jobSubmitReq.getConfProp());
        jobDescriptor.setJobName(jobSubmitReq.getJobName());

        return jobDescriptor;
    }
}
