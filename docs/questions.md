## 常见问题

### 1.编译找不到DB2、达梦、gbase、ojdbc8等驱动包

解决办法：在$FLINKX_HOME/jars目录下有这些驱动包，可以手动安装，也可以使用插件提供的脚本安装：

```bash
## windows平台
./install_jars.bat

## unix平台
./install_jars.sh
```

### 2.FlinkX版本需要与Flink版本保持一致

1.8_release版本对应flink1.8 1.10_release版本对应flink1.10 版本 不对应在standalone和yarn session模式提交时，会报错： Caused by: java.io.InvalidClassException: org.apache.flink.api.common.operators.ResourceSpec; incompatible types for field cpuCores

### 3.移动FlinkX lib目录下的Launcher包后，任务启动报错：错误: 找不到或无法加载主类

FlinkX启动脚本里面找的是lib目录下的所有jar包，而移动后lib中含有其他的jar包，这些jar包没有主类，因此报错 可以使用如下的命令运行： java -cp flinkx/lib/flinkx-client.jar com.dtstack.flinkx.launcher.Launcher -mode local -job /opt/flink/flink/deps/job/stream.json -flinkxDistDir flinkx/flinkx-dist

### 4. 关于编译flinkx core报错Failed to read artifact descriptor for com.google.errorprone:javac-shaded

报错信息：

```
[ERROR] Failed to execute goal com.diffplug.spotless:spotless-maven-plugin:2.4.2:check (spotless-check) on project flinkx-core: 
Execution spotless-check of goal com.diffplug.spotless:spotless-maven-plugin:2.4.2:check failed: Unable to resolve dependencies: 
Failed to collect dependencies at com.google.googlejavaformat:google-java-format:jar:1.7 -> com.google.errorprone:javac-shaded:jar:9+181-r4173-1: 
Failed to read artifact descriptor for com.google.errorprone:javac-shaded:jar:9+181-r4173-1: Could not transfer artifact 
com.google.errorprone:javac-shaded:pom:9+181-r4173-1 from/to aliyunmaven (https://maven.aliyun.com/repository/public): 
Access denied to:https://maven.aliyun.com/repository/public/com/google/errorprone/javac-shaded/9+181-r4173-1/javac-shaded-9+181-r4173-1.pom -> [Help 1]
```

解决：

https://repo1.maven.org/maven2/com/google/errorprone/javac-shaded/9+181-r4173-1/javac-shaded-9+181-r4173-1.jar
从这个地址下载javac-shaded-9+181-r4173-1.jar， 临时放到chunjun根目录下jars目录里，然后在源码根目录下 执行安装依赖包命令如下：

```
mvn install:install-file -DgroupId=com.google.errorprone -DartifactId=javac-shaded -Dversion=9+181-r4173-1 -Dpackaging=jar -Dfile=./jars/javac-shaded-9+181-r4173-1.jar
