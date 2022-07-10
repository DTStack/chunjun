import { Tooltip } from "@mantine/core"
import { FaHome, FaBookOpen, FaBoxes, FaDownload } from 'react-icons/fa';
import { FcHome } from 'react-icons/fc'
import { BiCodeBlock } from 'react-icons/bi'
import { MdDarkMode } from 'react-icons/md'
import { HiSun } from 'react-icons/hi'
import React from 'react'
export const headerList = [
  {
    name: "首页",
    path: "/",
    icon: <Tooltip className="flex items-center" position="bottom" label="首页">
      <FcHome className="text-xl" ></FcHome>
    </Tooltip>
  },
  {
    name: "文档",
    path: "/documents",
    key: "documents",
    icon: <Tooltip className="flex items-center" position="bottom" label="文档">
      <FaBookOpen className="text-xl" />
    </Tooltip>
  },
  {
    name: "案例",
    path: [
      {
        name: "sql",
        link: "/examples/sql",
      },
      {
        name: "json",
        link: "/examples/json",
      },
    ],
    key: "examples",
    icon: <Tooltip className="flex items-center" position="bottom" label="案例">
      <BiCodeBlock className="text-xl" />
    </Tooltip>
  },
  // {
  //   name: "博客",
  //   path: "/blogs",
  //   key: "blogs",
  // },
  {
    name: "下载",
    path: null,
    url: "https://github.com/DTStack/chunjun/releases",
    key: "download",
    icon: <Tooltip className="flex items-center" position="bottom" label="下载">
      <FaDownload className="text-xl" />
    </Tooltip>
  },
  // {
  //   name: '主题',
  //   path: '//',
  //   renderIcon: ({ mode }) => {
  //     return (
  //       <>
  //         <Tooltip className="flex items-center" position="bottom" label="主题">
  //           {mode === 'dark' ? <MdDarkMode className="text-xl" style={{ color: 'yellow' }} /> : <HiSun className="text-xl" style={{ color: 'orange' }} />}
  //         </Tooltip>
  //       </>
  //     )
  //   },
  //   clickfn: 'themeToggle'
  // }
  // {
  //   name: "Roadmap",
  //   path: "/roadmap",
  //   key: "roadmap",
  // },
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
