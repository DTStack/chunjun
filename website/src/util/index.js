export const headerList = [
  {
    name: "首页",
    path: "/",
  },
  {
    name: "文档",
    path: "/documents",
    key: "documents",
  },
  {
    name: "案例",
    path: "/examples",
    key: "examples",
  },
  {
    name: "博客",
    path: "/blogs",
    key: "blogs",
  },
  {
    name: "下载",
    path: "/download",
    key: "download",
  },
  {
    name: "Roadmap",
    path: "/roadmap",
    key: "roadmap",
  },
]

export function buildMenu(nodes) {
  let id = 1
  const root = { children: [] }
  function linkToRoot(structrue, node) {
    let rootRef = root

    for (let i = 0; i < structrue.length - 1; i++) {
      let dirname = structrue[i]
      let nextRef = rootRef.children.find(item => item.name === dirname)
      if (!nextRef) {
        nextRef = {
          type: "dir",
          name: dirname,
          id: id++,
          children: [],
          parent: rootRef,
        }
        rootRef.children.push(nextRef)
      }
      rootRef = nextRef
    }
    rootRef.children.push({
      type: "file",
      name: node.name,
      data: node,
      parent: rootRef,
    })
  }
  for (let i = 0; i < nodes.length; i++) {
    let node = nodes[i]
    let structrue = node.relativePath.split("/")
    if (structrue.length === 1) {
      root.children.push({
        type: "file",
        name: node.name,
        data: node,
        parent: root,
      })
    } else {
      linkToRoot(structrue, node)
    }
  }

  return root
}

export function getFileArr(list) {
  const res = []
  travel(list)
  function travel(list) {
    list.forEach(item => {
      if (item.type === "file") {
        res.push(item)
      } else {
        travel(item.children || [])
      }
    })
  }
  return res
}
