mvn -T 4 clean package -DskipTests
rm -rf flinkx-thin &&  mkdir flinkx-thin
cp -r bin flinkx-thin
cp -r flinkx-dist flinkx-thin
cp -r lib flinkx-thin

tar cvzf flinkx-thin.tar.gz flinkx-thin
