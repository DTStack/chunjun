package com.dtstack.flinkx.enums;

/**
 * @author wuzhongjian_yewu@cmss.chinamobile.com
 * @date 2021-12-16
 */
public enum WriteType {

    /**
     * Data is written synchronously to a Alluxio worker and the under storage system.
     */
    CACHE_THROUGH,

    /**
     * Data is written synchronously to a Alluxio worker.
     * No data will be written to the under storage. This is the default write type.
     */
    MUST_CACHE,

    /**
     * Default，Data is written synchronously to the under storage. No data will be written to Alluxio.
     */
    THROUGH,

    /**
     * Data is written synchronously to a Alluxio worker and asynchronously to the under storage system. Experimental.
     */
    ASYNC_THROUGH

}
