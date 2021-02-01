package com.dtstack.flinkx.metadatapostgresql.utils;



/**
 *
 * @author shitou
 * @date 2020/12/10 16:54
 */
public class CommonUtils {


    /**
     * 用dbName替换url中的dbName
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

    /**
     * 根据创建索引的SQL语句得到索引类型
     *@param sql: SQL语句
     *@return 索引类型
     *
    **/
    public  static String indexType(String sql){
        //sql:CREATE UNIQUE INDEX xxxx ON xxxx USING btree (id)
        String result = sql.toLowerCase();
        String[] s = result.split(" ");
        for (int i = 0; i < s.length; i++ ){
            if("using".equals(s[i])){
                result = s[i + 1];
            }
        }
        return result;
    }

}
