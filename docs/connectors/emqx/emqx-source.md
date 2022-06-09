# Emqx Source

## 一、介绍
从emq实时读取数据

## 二、支持版本
主流版本


## 三、插件名称
| Sync | emqxsource、emqxreader |
| --- | --- |
| SQL | emqx-x |

## 四、参数说明
### 1、Sync
- **broker**
  - 描述：连接URL信息
  - 必选：是
  - 参数类型：string
  - 默认值：无
<br /> 
    
- **topic**
  - 描述：订阅主题
  - 必选：是
  - 参数类型：string
  - 默认值：无
<br />
    
- **username**
  - 描述：认证用户名
  - 必选：否
  - 参数类型：string
  - 默认值：无
<br />
    
- **password**
  - 描述：认证密码
  - 必选：否
  - 参数类型：string
  - 默认值：无
<br />
    
- **isCleanSession**
  - 描述：是否清除session
    - false：MQTT服务器保存于客户端会话的的主题与确认位置；
    - true：MQTT服务器不保存于客户端会话的的主题与确认位置
  - 必选：否
  - 参数类型：boolean
  - 默认值：true
<br />
    
- **qos**
  - 描述：服务质量
    - 0：AT_MOST_ONCE，至多一次；
    - 1：AT_LEAST_ONCE，至少一次；
    - 2：EXACTLY_ONCE，精准一次；
  - 必选：否
  - 参数类型：int
  - 默认值：2
<br />
    
- **codec**
  - 描述：编码解码器类型，支持 json、plain
    - plain：将kafka获取到的消息字符串存储到一个key为message的map中，如：`{"message":"{\"key\": \"key\", \"message\": \"value\"}"}`
    - plain：将kafka获取到的消息字符串按照json格式进行解析
      - 若该字符串为json格式
        - 当其中含有message字段时，原样输出，如：`{"key": "key", "message": "value"}`
        - 当其中不包含message字段时，增加一个key为message，value为原始消息字符串的键值对，如：`{"key": "key", "value": "value", "message": "{\"key\": \"key\", \"value\": \"value\"}"}`
      - 若改字符串不为json格式，则按照plain类型进行处理
  - 必选：否
  - 参数类型：string
  - 默认值：plain
<br />

### 2、SQL
- **connector**
  - 描述：emqx-x
  - 必选：是
  - 参数类型：String
  - 默认值：无
<br />
    
- **broker**
  - 描述：连接信息tcp://localhost:1883
  - 必选：是
  - 参数类型：String
  - 默认值：无
<br />
    
- **topic**
  - 描述：主题
  - 必选：是
  - 参数类型：String
  - 默认值：无
<br />
    
- **isCleanSession**
  - 描述：是否清除session
    - false：MQTT服务器保存于客户端会话的的主题与确认位置；
    - true：MQTT服务器不保存于客户端会话的的主题与确认位置
  - 必选：否
  - 参数类型：String
  - 默认值：true
<br />
    
- **qos**
  - 描述：服务质量
    - 0：AT_MOST_ONCE，至多一次；
    - 1：AT_LEAST_ONCE，至少一次；
    - 2：EXACTLY_ONCE，精准一次；
  - 必选：否
  - 参数类型：String
  - 默认值：2
<br />
    
- **username**
  - 描述：username
  - 必选：否
  - 参数类型：String
  - 默认值：无
<br />
    
- **password**
  - 描述：password
  - 必选：否
  - 参数类型：String
  - 默认值：无
<br />
    
- **format**
  - 描述：数据来源格式
  - 必选：否
  - 参数类型：String
  - 默认值：json
<br />
    
## 五、数据类型
| 支持 | BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、DECIMAL、STRING、VARCHAR、CHAR、TIMESTAMP、DATE、BINARY、ARRAY、MAP、STRUCT、LIST、ROW |
| --- | --- |
| 暂不支持 | 其他 |


## 六、脚本示例
见项目内`chunjun-examples`文件夹。
