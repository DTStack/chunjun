# 打包找不到db2和oracle相关驱动包临时解决办法

下载这连个驱动包，上传到本地仓库：

db2：[下载](db2jcc-3.72.44.jar)

oracle：[下载](ojdbc8-12.2.0.1.jar)

gbase：[下载](gbase-8.3.81.53.jar)

达梦：[下载](Dm7JdbcDriver18.jar)

然后上传到本地仓库：

```
mvn install:install-file -DgroupId=com.ibm.db2 -DartifactId=db2jcc -Dversion=3.72.44 -Dpackaging=jar -Dfile=db2jcc-3.72.44.jar

mvn install:install-file -DgroupId=com.github.noraui -DartifactId=ojdbc8 -Dversion=12.2.0.1 -Dpackaging=jar -Dfile=ojdbc8-12.2.0.1.jar

mvn install:install-file -DgroupId=com.esen.jdbc -DartifactId=gbase -Dversion=8.3.81.53 -Dpackaging=jar -Dfile=gbase-8.3.81.53.jar

mvn install:install-file -DgroupId=com.dm -DartifactId=Dm7JdbcDriver18 -Dversion=7.6.0.197 -Dpackaging=jar -Dfile=Dm7JdbcDriver18.jar
```

说明：这几个驱动包在我们自己搭建的仓库里有，并且这几个版本的驱动包在已经在生产环境中使用，所以不能很快修改版本，需要做相关测试，我们会在后期的版本中修改这两个驱动包的版本，可以先暂时下载安装驱动来解决。
