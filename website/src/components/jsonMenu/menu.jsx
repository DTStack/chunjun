import React from "react"
import { graphql, Link, useStaticQuery } from "gatsby"
import { Accordion, AppShell } from "@mantine/core"
import AppFooter from "../../components/AppFooter"
import Seo from "../seo"
import { buildChildren } from "../../util"

/**
 * @description 目录解析
 * @param {nodes} 文件信息
 * @reference
 */

const MenuLayout = ({ children }) => {
  //nodes 是文档list的相关信息, 文档的详细路由是  /documents/{name}
  const data = useStaticQuery(graphql`
    query MyQuery2 {
      allFile(filter: { sourceInstanceName: { eq: "examples" }, extension: { eq: "json" }, ctime: {} }) {
        edges {
          node {
            id
            name
            parent {
              id
              ... on Directory {
                id
                name
              }
            }
            relativePath
            ctime
            modifiedTime
          }
        }
      }
    }
  `)

  function buildMenu(nodes) {
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

  const menuData = buildMenu(data.allFile.edges.map(item => item.node))
  const currentPage = window.location.href.split("/").pop()

  const buildChildren = children => {
    return children.map(c => {
      if (c.type === "dir") {
        return (
          <Accordion iconPosition="right">
            <Accordion.Item label={c.name}>{buildChildren(c.children)}</Accordion.Item>
          </Accordion>
        )
      } else {
        return (
          <Link to={`/examples/${c.data.id}`} className={`w-full px-[20px] text-sm rounded-sm cursor-pointer hover:bg-gray-100 h-[48px] flex items-center ${c.data.id === currentPage ? "active" : null}`}>
            {c.name}
          </Link>
        )
      }
    })
  }

  const asideMenu = menu => {
    const { children } = menu
    return (
      <aside className="md:inline-block hidden shadow-sm h-screen overflow-y-auto px-[12px] py-[20px]">
        {children.map(item => {
          return item.type === "file" ? (
            <Link to={`/examples/${item.data.id}`} className={`w-full text-sm px-[20px] rounded-sm cursor-pointer hover:bg-gray-100 h-[48px] flex items-center ${item.data.id === currentPage ? "active" : null}`}>
              {item.name}
            </Link>
          ) : (
            <Accordion iconPosition="right">
              <Accordion.Item label={item.name}>{buildChildren(item.children)}</Accordion.Item>
            </Accordion>
          )
        })}
      </aside>
    )
  }

  return (
    <>
      <Seo title="纯钧" />
      <AppShell padding={"sm"} navbar={menuData.children.length > 0 && asideMenu(menuData)}>
        {children}
      </AppShell>
      <AppFooter />
    </>
  )
}

export default MenuLayout
