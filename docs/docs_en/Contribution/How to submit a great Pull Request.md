# How to submit a great Pull Request

在 github 上提交 pr 是参与 ChunJun 开源项目的一个重要途径，小伙伴们在使用中的一些功能上 feature 或者 bug 都可以向社区提交 pr 贡献代码，也可以根据已有的 issue 提供自己的解决方案。下面给大家带来提交一个优秀 PR 的步骤。

## 第一步：fork chunjun 到自己的 github 仓库

![image](/doc/pr/pr1.png)

点击 fork 后就可以在自己仓库中看到以你名字命名的 chunjun 项目了：

![image](/doc/pr/pr2.png)

## 第二步：clone chunjun 到本地 IDE

![image](/doc/pr/pr3.png)

## 第三步：将 DTStack/chunjun 设置为本地仓库的远程分支 upstream

```shell
$ cd chunjun
# add upstream
$ git remote add upstream https://github.com/DTStack/chunjun.git
# 查看远程仓库设置
$ git remote -v
origin  https://github.com/your_name/chunjun.git (fetch)
origin  https://github.com/your_name/chunjun.git (push)
upstream    https://github.com/DTStack/chunjun.git (fetch)
upstream    https://github.com/DTStack/chunjun.git (push)
```

## 第四步：提交代码

任何一个提交都要基于最新的分支
**切换分支**

```shell
# Fetch branches from upstream.
$ git remote update upstream -p
# Checkout a new branch.
$ git checkout -b branch_name
# Pull latest code into your own branch.
$ git pull upstream master:branch_name
```

**本地修改代码后，提交 commit**

- commit message 规范：
  [commit_type-#issue-id] [module] message
- commit_type:
  - feat：表示是一个新功能（feature)
  - hotfix：hotfix，修补 bug
  - docs：改动、增加文档
  - opt：修改代码风格及 opt imports 这些，不改动原有执行的代码
  - test：增加测试
- eg:[hotfix-#12345][mysql] Fix mysql time type loses precision.

注意：
（1）commit 需遵循规范，给维护者减少维护成本及工作量，对于不符合规范的 commit，我们不与合并；
（2）对于解决同一个 Issue 的 PR，只能存在一个 commit message，如果出现多次提交的 message，我们希望你能将 commit message 'squash' 成一个；
（3）message 尽量保持清晰简洁，但是也千万不要因为过度追求简洁导致描述不清楚，如果有必要，我们也不介意 message 过长，前提是，能够把解决方案、修复内容描述清楚；

```shell
# 提交commit前先进行代码格式化
$ mvn spotless:apply
$ git commit -a -m "<you_commit_message>"
```

**rebase 远程分支**

这一步很重要，因为我们仓库中的 chunjun 代码很有可能已经落后于社区，所以在 push commit 前需要 rebase，保证当前 commit 是基于社区最新的代码，很多小伙伴没有这一步导致提交的 pr 当中包含了其他人的 commit

```shell
$ git fetch upstream
$ git rebase upstream/branch_name
```

\*rebase 后有可能出现代码冲突，一般是由于多人编辑同一个文件引起的，只需要根据提示打开冲突文件对冲突部分进行修改，将提示的冲突文件的冲突都解决后，执行

```shell
$ git add .
$ git rebase --continue
```

依此往复，直至屏幕出现类似 rebase successful 字样即可

\*rebase 之后代码可能无法正常推送，需要`git push -f` 强制推送，强制推送是一个有风险的操作，操作前请仔细检查以避免出现无关代码被强制覆盖的问题

**push 到 github 仓库**

```shell
$ git push origin branch_name
```

## 第五步：提交 pr

以笔者修复 kafka 写入过程中出现空指针问题为例，经过步骤四笔者已经把代码提交至笔者自己仓库的 master 分支

![image](/doc/pr/pr4.png)

进入 chunjun 仓库页面，点击 Pull Request

![image](/doc/pr/pr5.png)

![image](/doc/pr/pr6.png)

选择 head 仓库和 base 仓库以及相应的分支

![image](/doc/pr/pr7.png)

填写 pr 信息，pr 信息应该尽量概括清楚问题的前因后果，如果存在对应 issue 要附加 issue 地址，保证问题是可追溯的

![image](/doc/pr/pr8.png)

![image](/doc/pr/pr9.png)

PR 提交成功后需要一段时间代码 review，可以耐心等待一下项目维护者 review 后合入，或者在 PR 评论区艾特相关人员。
