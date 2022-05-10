FlinkX Docker
============
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# build step

1.使用git工具把项目clone到本地

```
git clone https://github.com/DTStack/flinkx.git
```

2.编译插件包

```
cd flinkx
sh build/build.sh
```

3.拷贝构建的插件包到镜像构建目录下

```
cp -r flink-dist ./flinkx-docker/docker
```

4.构建镜像

```
cd ./flinkx-docker/docker
docker build -t ${image_name} .
```

镜像构建完成后可推送到docker hub供后续使用



