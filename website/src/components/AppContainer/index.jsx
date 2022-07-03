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
  const buildChildren = children => {
    return children.map(c => {
      if (c.type === "dir") {
        return (
          <Accordion key={c.name} iconPosition="right">
            <Accordion.Item label={c.name}>
              {buildChildren(c.children)}
            </Accordion.Item>
          </Accordion>
        )
      } else {
        return (
          <Link
            activeClassName="active"
            key={c.data.id}
            to={`${category}/${c.data.id}`}
            className={`w-full pl-[20px] text-sm rounded-sm cursor-pointer hover:bg-gray-100 h-[48px] flex items-center`}
          >
            {c.name}
          </Link>
        )
      }
    })
  }

  const asideMenu = menu => {
    const { children } = menu
    return (
      <Navbar
        className="md:inline-block sticky top-[90px]    hidden  px-0 no-scrollbar"
        hiddenBreakpoint="sm"
        width={{ sm: 200, lg: 256 }}
        p="xs"
        style={{ zIndex: "1", overflowY: "auto", height: "calc(100vh - 90px)" }}
      >
        {children.map(item => {
          return item.type === "file" ? (
            <Link
              activeClassName="active"
              to={`${category}/${item.data.id}`}
              key={item.data.id}
              className={`w-full text-base pl-[20px] rounded-sm cursor-pointer hover:bg-gray-100 h-[48px] flex items-center`}
            >
              {item.name}
            </Link>
          ) : (
            <Accordion key={item.id} iconPosition="right" className="uppercase">
              <Accordion.Item label={item.name} className="capitalize">
                {buildChildren(item.children)}
              </Accordion.Item>
            </Accordion>
          )
        })}
      </Navbar>
    )
  }

  return (
    <>
      <Seo title="纯钧" />

      <AppHeader />
      <div className="container-content relative border justify-between bg-white flex z-20 shadow-lg">
        {menuData.children.length > 0 && asideMenu(menuData)}
        {children}
      </div>

      <AppFooter />
    </>
  )
}

export default AppContainer
