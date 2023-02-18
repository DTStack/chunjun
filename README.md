# ChunJun

<p align="left">
  <img src="https://img.shields.io/github/stars/DTStack/chunjun?style=social" alt="npm version" />
  <img src="https://img.shields.io/github/license/DTStack/chunjun" alt="license" />
  <a href="https://github.com/DTStack/chunjun/releases"><img src="https://img.shields.io/github/downloads/DTStack/chunjun/total" alt="npm downloads" /></a>
  <img src="https://img.shields.io/gitlab/coverage/DTStack/chunjun/master" alt="master coverage" />
</p>

[![EN doc](https://img.shields.io/badge/document-English-blue.svg)](README.md)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](README_CH.md)

## Introduce

ChunJun is a distributed integration framework, and currently is based on Apache Flink. It was initially known as FlinkX and renamed ChunJun on February 22, 2022. It can realize data synchronization and calculation between various heterogeneous data sources. ChunJun has been deployed and running stably in thousands of companies so far.

Official website of ChunJun: https://dtstack.github.io/chunjun/

## Features of ChunJun

ChunJun abstracts different databases into reader/source plugins, writer/sink plugins and lookup plugins, and it has the following features:

- Based on the real-time computing engine--Flink, and supports JSON template and SQL script configuration tasks. The SQL script is compatible with Flink SQL syntax;
- Supports distributed operation, support flink-standalone, yarn-session, yarn-per job and other submission methods;
- Supports Docker one-click deployment, support deploy and run on k8s;
- Supports a variety of heterogeneous data sources, and supports synchronization and calculation of more than 20 data sources such as MySQL, Oracle, SQLServer, Hive, Kudu, etc.
- Easy to expand, highly flexible, newly expanded data source plugins can integrate with existing data source plugins instantly, plugin developers do not need to care about the code logic of other plugins;
- Not only supports full synchronization, but also supports incremental synchronization and interval training;
- Not only supports offline synchronization and calculation, but also compatible with real-time scenarios;
- Supports dirty data storage, and provide indicator monitoring, etc.;
- Cooperate with the flink checkpoint mechanism to achieve breakpoint resuming, task disaster recovery;
- Not only supports synchronizing DML data, but also supports DDL synchronization, like 'CREATE TABLE', 'ALTER COLUMN', etc.;

## Build And Compilation

### Get the code

Use the git to clone the code of ChunJun

```shell
git clone https://github.com/DTStack/chunjun.git
```

### build

Execute the command in the project directory.

```shell
./mvnw clean package
```

Or execute

```shell
sh build/build.sh
```

### Common problem

#### Compiling module 'ChunJun-core' then throws 'Failed to read artifact descriptor for com.google.errorprone:javac-shaded'

Error message：

```java
[ERROR]Failed to execute goal com.diffplug.spotless:spotless-maven-plugin:2.4.2:check(spotless-check)on project chunjun-core:
        Execution spotless-check of goal com.diffplug.spotless:spotless-maven-plugin:2.4.2:check failed:Unable to resolve dependencies:
        Failed to collect dependencies at com.google.googlejavaformat:google-java-format:jar:1.7->com.google.errorprone:javac-shaded:jar:9+181-r4173-1:
        Failed to read artifact descriptor for com.google.errorprone:javac-shaded:jar:9+181-r4173-1:Could not transfer artifact
        com.google.errorprone:javac-shaded:pom:9+181-r4173-1 from/to aliyunmaven(https://maven.aliyun.com/repository/public): 
        Access denied to:https://maven.aliyun.com/repository/public/com/google/errorprone/javac-shaded/9+181-r4173-1/javac-shaded-9+181-r4173-1.pom -> [Help 1]
```

Solution：
Download the 'javac-shaded-9+181-r4173-1.jar' from url 'https://repo1.maven.org/maven2/com/google/errorprone/javac-shaded/9+181-r4173-1/javac-shaded-9+181-r4173-1.jar', and then install locally by using command below:

```shell
mvn install:install-file -DgroupId=com.google.errorprone -DartifactId=javac-shaded -Dversion=9+181-r4173-1 -Dpackaging=jar -Dfile=./jars/javac-shaded-9+181-r4173-1.jar
```

## Quick Start

The following table shows the correspondence between the branches of ChunJun and the version of flink. If the versions are not aligned, problems such as 'Serialization Exceptions', 'NoSuchMethod Exception', etc. mysql occur in tasks.

| Branches     | Flink version |
|--------------|---------------|
| master       | 1.16.1        |
| 1.12_release | 1.12.7        |
| 1.10_release | 1.10.1        |
| 1.8_release  | 1.8.3         |

ChunJun supports running tasks in multiple modes. Different modes depend on different environments and steps. The following are

### Local

Local mode does not depend on the Flink environment and Hadoop environment, and starts a JVM process in the local environment to perform tasks.

#### Steps

Go to the directory of 'chunjun-dist' and execute the command below:

```shell
sh bin/chunjun-local.sh  -job $SCRIPT_PATH
```

The parameter of "$SCRIPT_PATH" means 'the path where the task script is located'.
After execute, you can perform a task locally.

note:
```
when you package in windows and run sh in linux , you need to execute command  sed -i "s/\r//g" bin/*.sh to fix the '\r' problems.
```

[Reference video](https://www.bilibili.com/video/BV1mT411g7fJ?spm_id_from=333.999.0.0)

### Standalone

Standalone mode depend on the Flink Standalone environment and does not depend on the Hadoop environment.

#### Steps
##### 1. add jars of chunjun 
1) Find directory of jars:
   if you build this project using maven, the directory name is 'chunjun-dist' ;
   if you download tar.gz file from release page, after decompression, the directory name would be like 'chunjun-assembly-${revision}-chunjun-dist'.

2) Copy jars to directory of Flink lib, command example:
```shell
cp -r chunjun-dist $FLINK_HOME/lib
```
Notice: this operation should be executed in all machines of Flink cluster, otherwise some jobs will fail because of ClassNotFoundException.


##### 2. Start Flink Standalone Cluster

```shell
sh $FLINK_HOME/bin/start-cluster.sh
```

After the startup is successful, the default port of Flink Web is 8081, which you can configure in the file of 'flink-conf.yaml'. We can access the 8081 port of the current machine to enter the flink web of standalone cluster.

##### 3. Submit task

Go to the directory of 'chunjun-dist' and execute the command below:

```shell
sh bin/chunjun-standalone.sh -job chunjun-examples/json/stream/stream.json
```

After the command execute successfully, you can observe the task staus on the flink web.

[Reference video](https://www.bilibili.com/video/BV1TT41137UV?spm_id_from=333.999.0.0)

### Yarn Session

YarnSession mode depends on the Flink jars and Hadoop environments, and the yarn-session needs to be started before the task is submitted.

#### Steps

##### 1. Start yarn-session environment

Yarn-session mode depend on Flink and Hadoop environment. You need to set $HADOOP_HOME and $FLINK_HOME in advance, and we need to upload 'chunjun-dist' with yarn-session '-t' parameter.

```shell
cd $FLINK_HOME/bin
./yarn-session -t $CHUNJUN_HOME -d
```

##### 2. Submit task

Get the application id $SESSION_APPLICATION_ID corresponding to the yarn-session through yarn web, then enter the directory 'chunjun-dist' and execute the command below:

```shell
sh ./bin/chunjun-yarn-session.sh -job chunjun-examples/json/stream/stream.json -confProp {\"yarn.application.id\":\"SESSION_APPLICATION_ID\"}
```

'yarn.application.id' can also be set in 'flink-conf.yaml'.
After the submission is successful, the task status can be observed on the yarn web.

[Reference video](https://www.bilibili.com/video/BV1oU4y1D7e7?spm_id_from=333.999.0.0)

### Yarn Per-Job

Yarn Per-Job mode depend on Flink and Hadoop environment. You need to set $HADOOP_HOME and $FLINK_HOME in advance.

#### Steps

The yarn per-job task can be submitted after the configuration is correct. Then enter the directory 'chunjun-dist' and execute the command below:

```shell
sh ./bin/chunjun-yarn-perjob.sh -job chunjun-examples/json/stream/stream.json
```

After the submission is successful, the task status can be observed on the yarn web.

## Docs of Connectors

For details, please visit：https://dtstack.github.io/chunjun/documents/

## Contributors

Thanks to all contributors! We are very happy that you can contribute Chunjun.

<a href="https://github.com/DTStack/chunjun/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=DTStack/chunjun"  alt="contributors"/>
</a>

## Contributor Over Time

[![Stargazers Over Time](https://contributor-overtime-api.git-contributor.com/contributors-svg?chart=contributorOverTime&repo=DTStack/chunjun)](https://git-contributor.com?chart=contributorOverTime&repo=DTStack/chunjun)

## License

ChunJun is under the Apache 2.0 license. Please visit [LICENSE](http://www.apache.org/licenses/LICENSE-2.0) for details.

## Contact Us

Join ChunJun Slack.
https://join.slack.com/t/chunjun/shared_invite/zt-1hzmvh0o3-qZ726NXmhClmLFRMpEDHYw
