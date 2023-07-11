## 纯钧官网 Next.js 重构版本

### 一. 项目信息

1. 基础框架: [Next.js](https://www.nextjs.cn/)
2. 包管理器: [pnpm](https://www.pnpm.cn/)
3. UI 组件: [Mantine](https://mantine.dev/)
4. CSS: [Tailwind CSS](https://www.tailwindcss.cn/)
5. 开发语言: [TypeScript](https://www.tslang.cn/)

### 二. 为什么会有这个版本？

&nbsp;&nbsp;众所周知，纯钧已经有了一个[官网](https://dtstack.github.io/chunjun/)，那么为什么还会有这个所谓的 Next.js 重构的版本呢？答案其实很简单：我们对之前的版本并不满意。因为不管是从开发上来看还是从实际使用体验上来看都又很多不尽人意的地方，所以才会有这个使用 nextjs 重构的版本。

### 三. 新的风格

1. 清晰
2. 明亮
3. 内敛
4. 留白

### 四. 文档修改
chunjun 文档使用 markdown 编写，存放在根目录下：
```
.
├── docs
    ├── docs_en
    │   ├── Advanced Features
    │   ├── ChunJun Connector
    │   ├── ChunJun Extend Data Format
    │   ├── Contribution
    │   └── ...
    └── docs_zh
        ├── ChunJun连接器
        ├── ChunJun拓展数据格式
        ├── 拓展功能
        ├── 开发者指南
        └── ...
```

#### 图片
文档图片存放在 website/public/doc 中。
```
.
├── website
    ├── public
        ├── doc
            ├── logo2d.svg
            └── ...
```
目前文档是部署在 github 站点上，访问地址前都需要增加 chunjun 项目前缀。完整地址为 `/chunjun + doc 文件夹下路径`， 如下:
```
![image](/chunjun/doc/dirty/dirty-conf.png)
```

### 四. 开发
在 website 目录下执行

安装依赖：
```shell
yarn
```

本地启动：
```shell
yarn dev
```

### 五. 部署

在 website 目录下执行

安装依赖：
```shell
yarn
```

部署 github 站点：
```shell
yarn deploy
```

### 六. 国际化

国际化使用的是 next-export-i18n，地址：https://github.com/martinkr/next-export-i18n

针对动态路由做了修改。

#### 文档目录

```
.
├── website
    ├── i18n
├── docs
    ├── docs_en
    └── docs_zh
```
- `i18n` 为翻译文件
- `docs` 内`dos_zh` 和 `docs_en` 用来分别存储**中文**和**英文**文档的源文件
