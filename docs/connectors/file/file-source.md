# File Source

## 一、介绍
file source仅支持从本地路径读取文件，支持读取原生flink所有文件类型。<br />

## 二、插件名称
| SQL | file-x |
| --- | --- |


## 三、参数说明
### 1、SQL

- **path**
   - 描述：读取的数据文件路径
   - 必选：是
   - 参数类型：string
   - 默认值：无
<br />

- **format**
   - 描述：文件的类型，和原生flink保持一致，支持原生所有类型
   - 必选：否
   - 参数类型：string
   - 默认值：csv
<br />

- **encoding**
   - 描述：字符编码
   - 必选：否
   - 字段类型：string
   - 默认值：`UTF-8`

## 四、数据类型
和原生flink数据类型保持一致<br />每种format所支持的数据类型请参考[flink官方文档](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/formats/)<br />

## 五、脚本示例
见项目内`chunjun-examples`文件夹。

