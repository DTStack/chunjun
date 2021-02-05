# 打包找不到驱动包解决办法

下载对应驱动包，上传到本地仓库：

db2：[下载](db2jcc-3.72.44.jar)

oracle：[下载](ojdbc8-12.2.0.1.jar)

gbase：[下载](gbase-8.3.81.53.jar)

达梦：[下载](Dm7JdbcDriver18.jar)

人大金仓：[下载](kingbase8-8.2.0.jar)

vertica：[下载](vertica-jdbc-9.1.1-0.jar)

然后上传到本地仓库：

```
## db2 driver
mvn install:install-file -DgroupId=com.ibm.db2 -DartifactId=db2jcc -Dversion=3.72.44 -Dpackaging=jar -Dfile=../jars/db2jcc-3.72.44.jar

## oracle driver
mvn install:install-file -DgroupId=com.github.noraui -DartifactId=ojdbc8 -Dversion=12.2.0.1 -Dpackaging=jar -Dfile=../jars/ojdbc8-12.2.0.1.jar

## gbase driver
mvn install:install-file -DgroupId=com.esen.jdbc -DartifactId=gbase -Dversion=8.3.81.53 -Dpackaging=jar -Dfile=../jars/gbase-8.3.81.53.jar

## dm driver
mvn install:install-file -DgroupId=dm.jdbc.driver -DartifactId=dm7 -Dversion=18.0.0 -Dpackaging=jar -Dfile=../jars/Dm7JdbcDriver18.jar

## kingbase driver
mvn install:install-file -DgroupId=com.kingbase8 -DartifactId=kingbase8 -Dversion=8.2.0 -Dpackaging=jar -Dfile=../jars/kingbase8-8.2.0.jar

## vertica driver
mvn install:install-file -DgroupId=fakepath -DartifactId=vertica-jdbc -Dversion=9.1.1-0 -Dpackaging=jar -Dfile=../jars/vertica-jdbc-9.1.1-0.jar
```
