# OceanBase CDC Source

## 一、介绍

OceanBase CDC 组件 [libobcdc](https://github.com/oceanbase/oceanbase/tree/master/tools/obcdc) 和 [oblogproxy](https://github.com/oceanbase/oblogproxy) 提供了拉取 OceanBase 增量 commit log 的能力。`chunjun-connector-oceanbasecdc` 在内部集成了 [oblogclient](https://github.com/oceanbase/oblogclient) 来连接 `oblogproxy`，获取相关的日志数据。

目前本插件只支持 DML 变更，sync 任务只支持 transformer 模式。

## 二、支持版本

OceanBase Log Proxy 社区版 1.0.0 及后续版本、公有云最新版本。

## 三、数据库配置

OceanBase Log Proxy 相关配置参见[代码仓库](https://github.com/oceanbase/oblogproxy)

## 四、插件名称

| Sync | oceanbasecdcsource、oceanbasecdcreader |
|------|---------------------------------------|
| SQL  | oceanbasecdc-x                        |

## 五、参数说明

在使用 `oblogclient` 连接 Log Proxy 时，需要提供相应的参数，使服务能够确定需要监听的 OceanBase 数据库的节点信息。

- 社区版 OceanBase，需要提供 `rootservice_list`。可以通过命令 `show parameters like 'rootservice_list'` 获取。
- 企业版/公有云版的 OceanBase，需要提供 `obconfig_url`。可以通过命令 `show parameters like 'obconfig_url'` 获取。

具体的参数说明：

### Sync

#### 基本配置

- **cat**
    - 描述：需要解析的数据更新类型，包括 insert、update、delete 三种
    - 注意：以英文逗号分割的格式填写。如果为空，解析所有数据更新类型
    - 必选：否
    - 字段类型：string
    - 默认值：`insert, delete, update`

- **timestampFormat**
    - 描述：指定输入输出所使用的timestamp格式，可选值：`SQL`、`ISO_8601`
    - 必选：否
    - 字段类型：string
    - 默认值：SQL

- **logProxyHost**
    - 描述：Log Proxy 的域名或 IP 地址
    - 必选：是
    - 参数类型：string
    - 默认值：无

- **logProxyPort**
    - 描述：Log Proxy 的端口
    - 必选：是
    - 参数类型：int
    - 默认值：无

#### libobcdc 配置

libobcdc 相关配置，参见[文档](https://github.com/oceanbase/oblogclient/blob/master/docs/quickstart/logproxy-client-tutorial.md#basic-usage)

- **obReaderConfig**
    - 描述：`libobcdc` 相关参数
    - 必选：是
    - 参数类型：结构体
    - 默认值：见文档

### SQL

#### 基本配置

- **cat**
    - 描述：需要解析的数据更新类型，包括 insert、update、delete 三种
    - 注意：以英文逗号分割的格式填写。如果为空，解析所有数据更新类型
    - 必选：否
    - 字段类型：string
    - 默认值：`insert, delete, update`

- **timestamp-format.standard**
    - 描述：同 Sync 中的`timestampFormat`参数，指定输入输出所使用的 timestamp 格式，可选值：`SQL`、`ISO_8601`
    - 必选：否
    - 字段类型：string
    - 默认值：SQL

- **log-proxy-host**
    - 描述：Log Proxy 的域名或 IP 地址
    - 必选：是
    - 参数类型：string
    - 默认值：无

- **log-proxy-port**
    - 描述：Log Proxy 的端口
    - 必选：是
    - 参数类型：int
    - 默认值：无

#### libobcdc 配置

libobcdc 相关配置，参见[文档](https://github.com/oceanbase/oblogclient/blob/master/docs/quickstart/logproxy-client-tutorial.md#basic-usage)

- **username**
    - 描述：数据源的用户名
    - 必选：是
    - 字段类型：string
    - 默认值：无

- **password**
    - 描述：数据源指定用户名的密码
    - 必选：是
    - 字段类型：string
    - 默认值：无

- **config-url**
    - 描述：前述的 `obconfig_url`，用于企业版或公有云版本的 OceanBase
    - 必选：否
    - 字段类型：string
    - 默认值：无

- **rootservice-list**
    - 描述：前述的 `rootservice_list`，用于社区版的 OceanBase
    - 必选：否
    - 字段类型：string
    - 默认值：无

- **table-whitelist**
    - 描述：监听的表格白名单，格式为 `tenant.db.table`，多个时以 `|` 分隔
    - 必选：是
    - 字段类型：string
    - 默认值：无

- **start-timestamp**
    - 描述：监听的起始时间戳，精确到秒
    - 必选：是
    - 字段类型：int
    - 默认值：无

- **timezone**
    - 描述：时区，需要与 OceanBase 中配置一致
    - 必选：否
    - 字段类型：string
    - 默认值：+08:00

- **working-mode**
    - 描述：`libobcdc`的工作模式，可选值为 `memory` 或 `storage`
    - 必选：否
    - 字段类型：string
    - 默认值：memory

## 六、数据类型

| OceanBase 类型                                          | Flink 类型        |
|-------------------------------------------------------|-----------------|
| BOOLEAN<br/> TINYINT(1)<br/> BIT(1)                   | BOOLEAN         |
| TINYINT                                               | TINYINT         |
| SMALLINT<br/> TINYINT UNSIGNED                        | SMALLINT        |
| SMALLINT UNSIGNED<br/> MEDIUMINT<br/> INT             | INT             |
| INT UNSIGNED<br/> BIGINT                              | BIGINT          |
| BIGINT UNSIGNED                                       | DECIMAL(20, 0)  |
| REAL<br/> FLOAT                                       | FLOAT           |
| DOUBLE                                                | DOUBLE          |
| NUMERIC(p, s)<br/>DECIMAL(p, s)<br/>where p <= 38     | DECIMAL(p, s)   |
| NUMERIC(p, s)<br/>DECIMAL(p, s)<br/>where 38 < p <=65 | STRING          |
| DATE                                                  | DATE            |
| YEAR                                                  | INT             |
| TIME [(p)]                                            | TIME [(p)]      |
| DATETIME [(p)] <br/> TIMESTAMP [(p)]                  | TIMESTAMP [(p)] |
| CHAR(n)                                               | CHAR(n)         |
| VARCHAR(n)                                            | VARCHAR(n)      |
| BIT(n)                                                | BINARY(⌈n/8⌉)   |
| BINARY(n) <br/> VARBINARY(N)                          | VARBINARY(N)    |
| TINYTEXT<br/> TEXT<br/> MEDIUMTEXT<br/> LONGTEXT      | STRING          |
| TINYBLOB<br/> BLOB<br/> MEDIUMBLOB<br/> LONGBLOB      | BYTES           |
| ENUM<br/> SET<br/> JSON                               | STRING          |

## 七、脚本示例

见项目内`chunjun-examples`文件夹。
