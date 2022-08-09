# Redis Source

## 一、介绍
支持读取redis的数据

## 二、支持的版本
redis2.9以上

## 三、插件名称
| sync | redis-x |
| sql | redisreader |

## 四、参数说明

#### 1、json

- **hostPort**
  - 描述：Redis的IP地址和端口
  - 必选：是
  - 默认值：localhost:6379
- **password**
  - 描述：数据源指定用户名的密码
  - 必选：否
  - 默认值：无
- **database**
  - 描述：要写入Redis数据库
  - 必选：否
  - 默认值：0
- **table-name**
  - 描述：需要匹配的key前缀
  - 必选：是
  - 默认值：无
- **type**
  - 描述：redis数据类型，目前只支持hash
  - 必选：是
  - 默认值：无
- **mode**
  - 描述：redis获取数据的方式,目前只支持hget
  - 必选：是
  - 默认值：无

#### 2、sql

- **url**
    - 描述：Redis的IP地址和端口
    - 必选：是
    - 默认值：localhost:6379
- **password**
    - 描述：数据源指定用户名的密码
    - 必选：否
    - 默认值：无
- **database**
    - 描述：要写入Redis数据库
    - 必选：否
    - 默认值：0
- **keyPrefix**
    - 描述：需要匹配的key前缀
    - 必选：是
    - 默认值：无
- **type**
    - 描述：redis数据类型，目前只支持hash
    - 必选：是
    - 默认值：无
- **mode**
    - 描述：redis获取数据的方式,目前只支持hget
    - 必选：是
    - 默认值：无
    


## 五、数据类型

目前只支持String


## 六、配置示例
见项目内`chunjun-examples`文件夹
