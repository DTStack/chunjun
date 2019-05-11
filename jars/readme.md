# 打包找不到db2和oracle相关驱动包临时解决办法

下载这连个驱动包，上传到本地仓库：

db2：[下载](jars/db2jcc-3.72.44.jar)

oracle：[下载](jars/ojdbc8-12.2.0.1.jar)

然后上传到本地仓库：

```
mvn install:install-file -DgroupId=com.ibm.db2 -DartifactId=db2jcc -Dversion=3.72.44 -Dpackaging=jar -Dfile=db2jcc-3.72.44.jar

mvn install:install-file -DgroupId=com.github.noraui -DartifactId=ojdbc8 -Dversion=12.2.0.1 -Dpackaging=jar -Dfile=ojdbc8-12.2.0.1.jar
```
