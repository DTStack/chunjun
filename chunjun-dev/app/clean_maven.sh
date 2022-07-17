#!/bin/sh
#!/bin/bash

REPO_PATH="/root/.m2/repository"

echo "正在清理"$REPO_PATH"目录下的.lastUpdated后缀文件"

find $REPO_PATH -name  "*.lastUpdated" | xargs rm -fr

echo "完成清理"
