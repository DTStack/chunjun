import AppHeader from "../AppHeader"
import AppFooter from "../AppFooter"
import { AppShell, Accordion, Navbar } from "@mantine/core"
import { Link } from "gatsby"
import React from "react"
import Seo from "../seo"

const AppContainer = ({ children, data, category }) => {
  //nodes 是文档list的相关信息, 文档的详细路由是  /documents/{name}

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
          <Link to={`${category}/${c.data.id}`} className={`w-full px-[20px] text-sm rounded-sm cursor-pointer hover:bg-gray-100 h-[48px] flex items-center ${c.data.id === currentPage ? "active" : null}`}>
            {c.name}
          </Link>
        )
      }
    })
  }

  const asideMenu = menu => {
    const { children } = menu
    return (
      <Navbar className="hidden md:inline-block" hiddenBreakpoint="sm" width={{ sm: 200, lg: 260 }} p="xs" style={{ zIndex: "1", height: "calc(100vh - 90px)", overflowY: "auto" }}>
        {children.map(item => {
          return item.type === "file" ? (
            <Link to={`${category}/${item.data.id}`} className={`w-full text-lg px-[20px] rounded-sm cursor-pointer hover:bg-gray-100 h-[48px] flex items-center ${item.data.id === currentPage ? "active" : null}`}>
              {item.name}
            </Link>
          ) : (
            <Accordion iconPosition="right">
              <Accordion.Item label={item.name}>{buildChildren(item.children)}</Accordion.Item>
            </Accordion>
          )
        })}
      </Navbar>
    )
  }

  return (
    <>
      <Seo title="纯钧" />
      <AppShell
        styles={{
          main: {
            height: "calc(100vh - 90px)",
            overflowY: "auto",
          },
        }}
        classNames={{
          main: "no-scrollbar",
        }}
        footer={<AppFooter />}
        header={<AppHeader />}
        padding={"sm"}
        navbar={menuData.children.length > 0 && asideMenu(menuData)}
      >
        {children}
      </AppShell>
    </>
  )
}

export default AppContainer
