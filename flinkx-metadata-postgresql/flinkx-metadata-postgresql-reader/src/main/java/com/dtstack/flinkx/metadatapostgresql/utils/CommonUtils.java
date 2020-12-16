package com.dtstack.flinkx.metadatapostgresql.utils;


/**
 * flinkx-all com.dtstack.flinkx.metadatapostgresql.utils
 *
 * @author shitou
 * @description //TODO
 * @date 2020/12/10 16:54
 */
public class CommonUtils {


    /**
     *@description 用dbName替换
     *@param url:
     *@param dbName: 数据库名
     *@return 新的url
     *
    **/
    public  static String dbUrlTransform(String url, String dbName){
        //"jdbc:postgresql://xxx.xxx.xxx.xxx/xxxx"
        int index = url.lastIndexOf("/");

        return url.substring(0,index + 1) + dbName;
    }


}
