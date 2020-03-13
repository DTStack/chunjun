package com.dtstack.flinkx.test.core.source;

import com.alibaba.fastjson.JSONObject;

/**
 * @author jiangbo
 * @date 2020/2/19
 */
public interface DataSource {

    /**
     * 准备数据库
     * @return 返回数据库连接信息
     */
    JSONObject prepare();
}
