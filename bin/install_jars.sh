#!/usr/bin/env bash

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