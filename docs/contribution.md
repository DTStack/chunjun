# 如何贡献FlinkX

本文面向FlinkX插件开发人员，尝试通过一个开发者的角度尽可能全面地阐述一个FlinkX插件所经历的过程，同时消除开发者的困惑，快速上手插件开发。

从数据流的角度来看FlinkX，可以理解为不同数据源的数据流通过对应的FlinkX插件处理，变成符合FlinkX数据规范的数据流；脏数据的处理可以理解为脏水流通过污水处理厂，变成符合标准，可以使用的水流，而对不能处理的水流收集起来。

插件开发不需要关注任务具体如何调度，只需要关注关键问题：

1. 数据源本身读写数据的正确性；
1. 如何合理且正确地使用框架；
1. 配置文件的规范；

<a name="29c80db5"></a>
## 开发环境

- Flink集群: 1.4及以上(单机模式不需要安装Flink集群）
- Java: JDK8及以上
- 操作系统：理论上不限，但是目前只编写了shell启动脚本，用户可以可以参考shell脚本编写适合特定操作系统的启动脚本。

开发之前，需要理解以下概念：

<a name="f1105542"></a>
## 逻辑执行概念
插件开发者不需要关心太多整个框架的具体运行，只需要关注数据源的读写，以及代码在逻辑上是怎么被执行的，方法什么时候被调用的。以下概念的理解对你快速开发会有帮助：

- **Job**：** Job**是FlinkX用以描述从一个源头到一个目的端的同步作业，是FlinkX数据同步的最小业务单元。
- **Internal**： 把**Job**拆分得到的最小执行单元。
- **InputSplit**：数据切片，是进入Internal的最小数据流单位。里面包含了基本数据信息和统计信息。
- **InputFormat**：读插件的执行单位。
- **OutputFormat**：写插件的执行单位。

<a name="209c6aed"></a>
## 任务执行模式

- 单机模式：对应Flink集群的单机模式
- standalone模式：对应Flink集群的分布式模式
- yarn模式：对应Flink集群的yarn模式
- yarnPer模式: 对应Flink集群的Per-job模式

在实际开发中，上述几种模式对插件的编写没有过多的影响，一般在本地LocalTest通过，将任务上传到Flink集群测试没有什么大问题。

<a name="92411f2e"></a>
## 插件入口类
插件的入口类需继承**DataReader**和**DataWriter**，在内部获取任务json传来的参数，通过相应的**Builder**构建对应**InputFormat**和**OutputFormat**实例

<a name="DataReader"></a>
### DataReader

```java
public class SomeReader extends DataReader {
    protected String oneParameter;
    public SomeReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
    }
    @Override
    public DataStream<Record> readData() {
        return null;
    }
}
```

reader类需继承DataReader，同时重写readData方法。在构造函数中获取任务json中构建InputFormat所需要的参数，代码案例如下：

构造方法

```java
protected String oneParameter;
public SomeReader(DataTransferConfig config, StreamExecutionEnvironment env) {
    super(config, env);
    // 首先通过jobconfig获取任务json中reader信息
    ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
    // 通过getParameter()获取相应的参数信息
    oneParameter = readerConfig.getParameter().getStringVal(SomeConfigKeys.KEY_PARAMETER);
}
```

重写readData方法

```java
@Override
public DataStream<Record> readData() {
    // 通过Builder构建InputFormat
    SomeInputFormatBuilder builder = new SomeInputFormatBuilder(new SomeInputFormat());
    // 一个setOneParameter()方法只set一个参数
    builder.setOneParameter(OneParameter);
    //调用createInput返回一个DataStream实例
    return createInput(builder.finish());
}
```

<a name="DataWriter"></a>
### DataWriter

```java
public class SomeWriter extends DataWriter {
    protected String oneParameter;
    public SomeWriter(DataTransferConfig config) {
        super(config);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Record> dataSet) {
        return null;
    }
}
```

和DataReader类似，writer需继承DataWriter，同时重写writeData方法。通常会创建一个ConfigKeys类，包含reader和writer所有需要的使用的任务json中参数的key。

构造方法

```java
protected String oneParameter;
public SomeWriter(DataTransferConfig config) {
    super(config);
    // 首先通过jobconfig获取jobjson中writer信息
    WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
	oneParameter = writerConfig.getParameter().getStringVal(SomeConfigKeys.KEY_PARAMETER);
}
```

重写writeData方法

```java
@Override
public DataStreamSink<?> writeData(DataStream<Record> dataSet) {
    // 通过Builder构建OutputFormat
    SomeOutputFormatBuilder builder = new SomeOutputFormatBuilder(new SomeOutputFormat());
    // 一个setOneParameter()方法只set一个参数
    builder.setOneParameter(OneParameter);
    //调用createInput返回一个DataSink实例
    return createInput(builder.finish());
}
```

<a name="e3fa8e04"></a>
### InputFormatBuilder的设计

需继承**RichInputFormatBuilder**

```java
public class SomeInputFormatBuilder extends RichInputFormatBuilder {
    /**
    * 首先实例化一个InputFormat实例，通过构造函数传递，通过set方法设置参数
    */
    protected SomeInputFormat format;
    //InputFormat构造函数，需要给实例化父类的format
    public SomeInputFormatBuilder(SomeInputFormat format){
        super.format = this.format = format;
    }
    //set方法示例，建议set方法内只给一个变量赋值
    public void setOneParameter(String oneParameter){
        this.oneParameter = oneParameter;
    }
    //重写checkFormat，检查一些必要参数设置是否正确
    @Override
    protected void checkFormat() {}
}
```

<a name="debbb760"></a>
### InputFormat的设计

需继承**RichInputFormat**，根据任务逻辑分别实现

```java
public class SomeInputFormat extends RichInputFormat {
    @override
    public void openInputFormat() {
        
    }
    
    @override
    public void closeInputFormat() {
    }
    
    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return null;
    }

    @Override
    protected void closeInternal() throws IOException {
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }
}
```


方法功能如下：
<a name="qxRB6"></a>
#### configure

- 调用位置：configure方法会在JobManager里构建执行计划的时候和在TaskManager里初始化并发实例后各调用一次；
- 作用：用于配置task的实例；
- 注意事项：不要在这个方法里写耗时的逻辑，比如获取连接，运行sql等，否则可能会导致akka超
<a name="P6eAb"></a>
#### createInputSplits

- 调用位置：在构建执行计划时调用；
- 作用：调用子类的逻辑生成数据分片；
- 注意事项：分片的数量和并发数没有严格对应关系，不要在这个方法里做耗时的操作，否则会导致akka超时异常；
<a name="Oas9f"></a>
#### getInputSplitAssigner

- 调用位置：创建分片后调用；
- 作用：获取分片分配器，同步插件里使用的是DefaultInputSplitAssigner，按顺序返回分配给各个并发实例；
- 注意事项：无；
<a name="TTNYz"></a>
#### openInternal

- 调用位置：开始读取分片时调用；
- 作用：用于打开需要读取的数据源，并做一些初始化；
- 注意事项：这个方法必须是可以重复调用的，因为同一个并发实例可能会处理多个分片；
<a name="hxlFZ"></a>
#### reachEnd和nextRecordInternal

- 调用位置：任务运行时，读取每条数据时调用；
- 作用：返回结束标识和下一条记录；
- 注意事项：无
<a name="bMWCr"></a>
#### closeInternal

- 调用位置：读取完一个分片后调用，至少调用一次；
- 作用：关闭资源；
- 注意事项：可重复调用，关闭资源做非null检查，因为程序遇到异常情况可能直接跳转到closeInternal；
<a name="FcsIE"></a>
#### openInputFormat

- 调用位置：创建分片之后调用；
- 作用：对整个InpurFormat资源做初始化；
- 注意事项：无；
<a name="1CyCJ"></a>
#### closeInputFormat

- 调用位置：当所有切片都执行完之后调用；
- 作用：关闭整个InputFormat的资源；
- 注意事项：无；

<a name="OutputFormatBuilder"></a>
### OutputFormatBuilder
需继承**RichOutputFormatBuilder**，和**InputFormatBuilder**相似

```java
public class SomeOutputFormatBuilder extends RichOutputFormatBuilder {
    /**
    * 首先实例化一个OutputFormat实例，通过构造函数传递，通过设计set方法设置参数
    * 如下演示
    */
    protected SomeOutputFormat format;
    
    public SomeOutputFormatBuilder(SomeOutputFormat format){
        super.format = this.format = format;
    }
    
    public void setOneParameter(String oneParameter){
        this.oneParameter = oneParameter;
    }
    
    //重写checkFormat，检查参数设置是否正确
    @Override
    protected void checkFormat() {}
}
```

<a name="OutputFormat"></a>
### OutputFormat
需继承**RichOutputFormat**

```java
public class SomeOutputFormat extends RichOutputFormat {
 	@Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {}
    
    @Override
    protected void writeSingleRecordInternal(Record record) {
    }
    
    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
    }
}
```

各方法的执行逻辑如下：

openInternal -> writeSingleRecordInternal / writeMultipleRecordsInternal

对于是执行writeSingleRecordInternal 还是writeMultipleRecordsInternal，关键参数是batchInterval，当batchInterval=1 时，框架调用writeSingleRecordInternal；当batchInterval > 1 且 record != null时，则调用writeMultipleRecordsInternal

方法功能如下：
<a name="vaLIS"></a>
#### openInternal

- 调用位置：开始写入使用
- 作用：用于打开需要读取的数据源，并做一些初始化；
- 注意事项：无；
<a name="Zbwct"></a>
#### writerSingleRecordInternal

- 调用位置：openInernal之后调用，开始写入数据
- 作用：向数据源写入一条数据
- 注意事项：无；
<a name="biBzT"></a>
#### writerMultipleRecordsInternal

- 调用位置：openInternal之后调用，开始写入多条数据
- 作用：向数据源写入多条数据，由batchInterval参数决定写入多少条
- 注意事项：无；

<a name="36baff55"></a>
## FlinkX数据结构
FlinkX延续了Flink原生的数据类型Row

```java
@PublicEvolving
public class Row implements Serializable{

	private static final long serialVersionUID = 1L;

	/** The array to store actual values. */
	private final Object[] fields;

	/**
	 * Create a new Row instance.
	 * @param arity The number of fields in the Row
	 */
	public Row(int arity) {
		this.fields = new Object[arity];
	}
}
```

<a name="c79d2697"></a>
## 任务json配置
配置中尽量减少不必要的参数，有些参数框架中已有默认值，配置文件中的值优先，模板如下

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "username": "",
            "password": "",
            "connection": [{
              "jdbcUrl": [""],
              "table": [
                ""
              ]
            }],
            "column": [{
              "name": "id",
              "type": "int"
            },{
              "name":"name",
              "type":"string"
            }]
          },
          "name": "mysqlreader"
        },
        "writer": {
          "parameter": {
            "print": true
          },
          "name": "streamwriter"
        }
      }
    ],
    "setting": {
      "errorLimit": {
      },
      "speed": {
      }
    }
  }
}
```

<a name="dd7607d3"></a>
## 如何设计配置参数
任务配置中`reader`和`writer`下`parameter`部分是插件的配置参数，插件的配置参数应当遵循以下原则：

- 驼峰命名：所有配置项采用驼峰命名法，首字母小写，单词首字母大写。
- 正交原则：配置项必须正交，功能没有重复，没有潜规则。
- 富类型：合理使用json的类型，减少无谓的处理逻辑，减少出错的可能。
  - 使用正确的数据类型。比如，bool类型的值使用`true`/`false`，而非`"yes"`/`"true"`/`0`等。
  - 合理使用集合类型，比如，用数组替代有分隔符的字符串。
- 类似通用：遵守同一类型的插件的习惯，比如关系型数据库的`connection`参数都是如下结构：
```
{
  "connection": [
    {
      "table": [
        "table_1",
        "table_2"
      ],
      "jdbcUrl": [
        "jdbc:mysql://127.0.0.1:3306/database_1",
        "jdbc:mysql://127.0.0.2:3306/database_1_slave"
      ]
    },
    {
      "table": [
        "table_3",
        "table_4"
      ],
      "jdbcUrl": [
        "jdbc:mysql://127.0.0.3:3306/database_2",
        "jdbc:mysql://127.0.0.4:3306/database_2_slave"
      ]
    }
  ]
}
```

<a name="cfd8aa31"></a>
## 如何处理脏数据
<a name="d6acc342"></a>
### 脏数据定义

1. Reader读到不支持的类型、不合法的值。
1. 不支持的类型转换，比如：`Bytes`转换为`Date`。
1. 写入目标端失败，比如：写mysql整型长度超长。

<a name="cfd8aa31-1"></a>
### 如何处理脏数据
框架会将脏数据临时存放起来。由DirtyDataManager实例写入临时存放脏数据文件中。

- path: 脏数据存放路径
- hadoopConfig: 脏数据存放路径对应hdfs的配置信息(hdfs高可用配置)

<a name="605265ae"></a>
## 加载原理

1. 框架扫描`plugin/reader`和`plugin/writer`目录，加载每个插件的`plugin.json`文件。
1. 以`plugin.json`文件中`name`为key，索引所有的插件配置。如果发现重名的插件或者不存在的插件，框架会异常退出。
1. 用户在插件中在`reader`/`writer`配置的`name`字段指定插件名字。框架根据插件的类型（`reader`/`writer`）和插件名称去插件的路径下扫描所有的jar，加入`classpath`。
1. 根据插件配置中定义的入口类，框架通过反射实例化对应的`Job`对象。

<a name="8e3d16c4"></a>
## 统一的目录结构
<a name="sdEM2"></a>
#### 项目目录层级
注意，插件Reader/Writer类需放在符合插件包名命名规则的reader下，如MysqlReader类需放在com.dtstack.flinkx.mysql.reader包下，具体命名规则参照 **项目命名规则** 内容
```xml
```
${Flinkx_HOME}
|-- bin       
|   -- flink
|   -- flinkx.sh 
|
|-- flinkx-somePlugin
    |-- flinkx-somePlugin-core
		|-- common 一些插件共用的类
		|-- exception 异常处理类
		|-- pom.xml 插件公用依赖
    |-- flinkx-somePlugin-reader
		|-- InputFormat
			|-- SomePluginInputFormat
			|-- SomePluginInputFormatBuiler
		|-- reader
			|-- SomePluginReader
	|-- flinkx-somePlugin-writer
		|-- OutputFormat
			|-- SomePluginOutputFormat
			|-- SomePluginOutputFormatBuiler
		|-- reader
			|-- SomePluginWriter
```
```
<a name="NMw2H"></a>
#### 项目命名规则

- 插件命名模板 [flinkx]-[dataSourceName]，例如flinkx-mysql
- 插件模块命名模板 [flinkx]-[dataSourceName]-[reader/writer/core]，例如flinkx-mysql-reader，flinkx-redis-writer
- 插件包名命名模板 [com.dtstack.flinkx.dataSource.xxxx]，例如com.dtstack.flinkx.mysql.reader，com.dtstack.flinkx.redis.inputformat
- 插件Reader/Writer类命名模板 [dataSource][Reader/Writer]，例如MysqlReader，RedisWriter，需注意，类似RestAPIWriter，MetaDataHive2Reader这样的命名是错误的，需改为RestapiWriter，Metadatahive2Reader

<a name="dd96ac2a"></a>
## 插件打包
进入项目根目录，使用maven打包：

windows平台

```
mvn clean package -DskipTests -Prelease -DscriptType=bat
```

unix平台

```
mvn clean package -DskipTests -Prelease -DscriptType=sh
```

打包结束后，项目根目录下会产生bin目录和plugins目录，其中bin目录包含FlinkX的启动脚本，plugins目录下存放编译好的数据同步插件包，之后就可以提交开发平台测试啦！
