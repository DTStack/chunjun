## 常见问题

### 1.编译找不到DB2、达梦、gbase、ojdbc8等驱动包

解决办法：在$FLINKX_HOME/jars目录下有这些驱动包，可以手动安装，也可以使用插件提供的脚本安装：

```bash
## windows平台
./install_jars.bat

## unix平台
./install_jars.sh
```

### 2.FlinkX版本需要与Flink版本保持一致，最好小版本也保持一致
| FlinkX分支 | Flink版本 |
| --- | --- |
| 1.8_release | Flink1.8.3 |
| 1.10_release | Flink1.10.1 |
| 1.11_release | Flink1.11.3 |
不对应在standalone和yarn session模式提交时，会报错：
Caused by: java.io.InvalidClassException: org.apache.flink.api.common.operators.ResourceSpec; incompatible types for field cpuCores

### 3.移动FlinkX lib目录下的Launcher包后，任务启动报错：错误: 找不到或无法加载主类
FlinkX启动脚本里面找的是lib目录下的所有jar包，而移动后lib中含有其他的jar包，这些jar包没有主类，因此报错
可以使用如下的命令运行：
java -cp /opt/flink/flink/deps/lib/flinkx-launcher-1.6.jar com.dtstack.flinkx.launcher.Launcher -mode local -job /opt/flink/flink/deps/job/stream.json -pluginRoot /opt/flink/flink/deps/syncplugins