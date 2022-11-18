# ChunJun

<p align="left">
  <img src="https://img.shields.io/github/stars/DTStack/chunjun?style=social" alt="npm version" />
  <img src="https://img.shields.io/github/license/DTStack/chunjun" alt="license" />
  <a href="https://github.com/DTStack/chunjun/releases"><img src="https://img.shields.io/github/downloads/DTStack/chunjun/total" alt="npm downloads" /></a>
  <img src="https://img.shields.io/gitlab/coverage/DTStack/chunjun/master" alt="master coverage" />
</p>

[![EN doc](https://img.shields.io/badge/document-English-blue.svg)](README.md)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](README_CH.md)

## 介绍

纯钧（ChunJun，原名FlinkX），是一款稳定、易用、高效、批流一体的数据集成框架，目前基于实时计算引擎Flink实现多种异构数据源之间的数据同步与计算，已在上千家公司部署且稳定运行。

官方网站：https://dtstack.github.io/chunjun/

## 特性

纯钧（ChunJun）将不同的数据库抽象成了reader/source 插件，writer/sink 插件和lookup 维表插件，其具有以下特点：

- 基于实时计算引擎Flink，支持JSON模版配置任务，兼容Flink SQL语法；
- 支持分布式运行，支持flink-standalone、yarn-session、yarn-per job等多种提交方式；
- 支持Docker一键部署，支持K8S 部署运行；
- 支持多种异构数据源，可支持MySQL、Oracle、SQLServer、Hive、Kudu等20多种数据源的同步与计算；
- 易拓展，高灵活性，新拓展的数据源插件可以与现有数据源插件即时互通，插件开发者不需要关心其他插件的代码逻辑；
- 不仅仅支持全量同步，还支持增量同步、间隔轮训；
- 批流一体，不仅仅支持离线同步及计算，还兼容实时场景；
- 支持脏数据存储，并提供指标监控等；
- 配合checkpoint实现断点续传；
- 不仅仅支持同步DML数据，还支持Schema变更同步；

## 源码编译

### 获取代码

使用git工具将纯钧项目代码下载在本地

```shell
git clone https://github.com/DTStack/chunjun.git
```

### 项目编译

在项目源码目录下执行

```shell
./mvnw clean package -DskipTests
```

或者执行

```shell
sh build/build.sh
```

### 多平台兼容

chunjun目前支持tdh和开源hadoop平台，对不同的平台有需要使用不同的maven命令打包

| 平台类型 |                                              | 含义                                    |
| -------- | -------------------------------------------- | --------------------------------------- |
| tdh      | mvn clean package -DskipTests -P default,tdh | 打包出inceptor插件以及default支持的插件 |
| default  | mvn clean package -DskipTests -P default     | 除了inceptor插件之外的所有插件          |

### 常见问题

#### 1.编译找不到DB2、达梦、Gbase、Ojdbc8等驱动包

解决办法：在$CHUNJUN_HOME/jars目录下有这些驱动包，可以手动安装，也可以使用插件提供的脚本安装：

```bash
## windows平台
./$CHUNJUN_HOME/bin/install_jars.bat

## unix平台
./$CHUNJUN_HOME/bin/install_jars.sh
```

#### 2. 关于编译ChunJun-core报错Failed to read artifact descriptor for com.google.errorprone:javac-shaded

报错信息：

```java
[ERROR]Failed to execute goal com.diffplug.spotless:spotless-maven-plugin:2.4.2:check(spotless-check)on project chunjun-core:
        Execution spotless-check of goal com.diffplug.spotless:spotless-maven-plugin:2.4.2:check failed:Unable to resolve dependencies:
        Failed to collect dependencies at com.google.googlejavaformat:google-java-format:jar:1.7->com.google.errorprone:javac-shaded:jar:9+181-r4173-1:
        Failed to read artifact descriptor for com.google.errorprone:javac-shaded:jar:9+181-r4173-1:Could not transfer artifact
        com.google.errorprone:javac-shaded:pom:9+181-r4173-1 from/to aliyunmaven(https://maven.aliyun.com/repository/public): 
        Access denied to:https://maven.aliyun.com/repository/public/com/google/errorprone/javac-shaded/9+181-r4173-1/javac-shaded-9+181-r4173-1.pom -> [Help 1]
```

解决：

https://repo1.maven.org/maven2/com/google/errorprone/javac-shaded/9+181-r4173-1/javac-shaded-9+181-r4173-1.jar
从这个地址下载javac-shaded-9+181-r4173-1.jar， 临时放到chunjun根目录下jars目录里，然后在源码根目录下 执行安装依赖包命令如下：

```shell
mvn install:install-file -DgroupId=com.google.errorprone -DartifactId=javac-shaded -Dversion=9+181-r4173-1 -Dpackaging=jar -Dfile=./jars/javac-shaded-9+181-r4173-1.jar
```

## 快速开始

以下表格是分支与flink版本之间的对应关系，如果版本没有对齐，可能会导致任务出现序列化异常，类冲突等问题。

| 分支         | flink 版本 |
| ------------ | ---------- |
| master       | 1.12.7     |
| 1.12_release | 1.12.7     |
| 1.10_release | 1.10.1     |
| 1.8_release  | 1.8.3      |

纯钧支持多种模式运行任务，不同模式下，所依赖的环境和步骤有所不同，以下内容是不同模式下的提交步骤：

### Local

Local 模式不依赖Flink环境和Hadoop环境，在本地环境启动一个JVM进程执行纯钧任务。

#### 提交步骤

进入到chunjun-dist 目录，执行命令

```shell
sh bin/chunjun-local.sh  -job chunjun-examples/json/stream/stream.json
```

即可执行一个简单的 **stream -> stream** 同步任务

注意:
```
如果你是在windows环境下打包，在linux上运行任务前需要执行 sed -i "s/\r//g" bin/*.sh 命令修复sh脚本中的  '\r' 问题。
```

[参考视频](https://www.bilibili.com/video/BV1mT411g7fJ?spm_id_from=333.999.0.0)

### Standalone

Standalone模式依赖Flink Standalone环境，不依赖Hadoop环境。

#### 提交步骤

##### 1. 添加chunjun依赖包
1) 根据实际情况找到依赖文件:
   通过maven编译的方式构建项目时，依赖文件目录为'chunjun-dist';
   通过官网下载压缩包解压使用时，依赖文件目录为解压后的目录，例如'chunjun-assembly-${revision}-chunjun-dist'

2) 将依赖文件复制到Flink lib目录下,例如
```shell
cp -r chunjun-dist $FLINK_HOME/lib
```
注意: 这个复制操作需要在所有Flink cluster机器上执行，否则部分任务会出现类找不到的错误。

##### 2. 启动Flink Standalone环境

```shell
sh $FLINK_HOME/bin/start-cluster.sh
```

启动成功后默认端口为8081，我们可以访问当前机器的8081端口进入standalone的flink web ui

##### 3. 提交任务

进入到本地chunjun-dist目录，执行命令

```shell
sh bin/chunjun-standalone.sh -job chunjun-examples/json/stream/stream.json
```

提交成功之后，可以在flink web ui 上观察任务情况；

[参考视频](https://www.bilibili.com/video/BV1TT41137UV?spm_id_from=333.999.0.0)

### Yarn Session

YarnSession 模式依赖Flink 和 Hadoop 环境，需要在任务提交之前启动相应的yarn session；

#### 提交步骤

##### 1. 启动Yarn Session环境

Yarn Session 模式依赖Flink 和 Hadoop 环境，需要在提交机器中提前设置好$HADOOP_HOME和$FLINK_HOME

我们需要使用yarn-session -t参数上传chunjun-dist

```shell
cd $FLINK_HOME/bin
./yarn-session -t $CHUNJUN_HOME -d
```

##### 2. 提交任务

通过yarn web ui 查看session 对应的application $SESSION_APPLICATION_ID，进入到本地chunjun-dist目录，执行命令

```shell
sh ./bin/chunjun-yarn-session.sh -job chunjun-examples/json/stream/stream.json -confProp {\"yarn.application.id\":\"SESSION_APPLICATION_ID\"}
```

yarn.application.id 也可以在 flink-conf.yaml 中设置；提交成功之后，可以通过 yarn web ui 上观察任务情况。

[参考视频](https://www.bilibili.com/video/BV1oU4y1D7e7?spm_id_from=333.999.0.0)

### Yarn Per-Job

Yarn Per-Job 模式依赖Flink 和 Hadoop 环境，需要在提交机器中提前设置好$HADOOP_HOME和$FLINK_HOME。

#### 提交步骤

Yarn Per-Job 提交任务配置正确即可提交。进入本地chunjun-dist目录，执行命令提交任务。

```shell
sh ./bin/chunjun-yarn-perjob.sh -job chunjun-examples/json/stream/stream.json
```

提交成功之后，可以通过 yarn web ui 上观察任务情况；

## 插件文档

详情请访问：https://dtstack.github.io/chunjun/documents/

## 贡献者

感谢所有的贡献者！

<a href="https://github.com/DTStack/chunjun/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=DTStack/chunjun"  alt="contributors"/>

## 开源协议

纯钧遵循Apache 2.0 开源协议。
