import AppHeader from "../AppHeader"
import AppFooter from "../AppFooter"
import { AppShell, Accordion, Navbar } from "@mantine/core"
import { Link, navigate } from "gatsby"
import React from "react"
import Seo from "../seo"
import "./darkMenu.scss"
import { Menu } from "antd"
import { ModeContext } from "../Context/index"
import {
  AppstoreOutlined,
  MailOutlined,
  SettingOutlined,
} from "@ant-design/icons"
import { useContext } from "react"

const AppContainer = ({ children, data, category }) => {
  let id = 0

  //nodes 是文档list的相关信息, 文档的详细路由是  /documents/{name}
  function buildMenu(nodes, deep = 0) {
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
            label: dirname,
            id: id++,
            key: id,
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
        label: node.name,
        id: node.id,
        key: node.id,
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
          label: node.name,
          data: node,
          key: node.id,
          parent: root,
        })
      } else {
        linkToRoot(structrue, node)
      }
    }
    console.log(root, "sql")
    return deep ? root.children[0] : root
  }

  const menuData = buildMenu(
    data.allFile.edges.map(item => item.node),
    data.deep
  )
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
            className={` `}
          >
            {c.name}
          </Link>
        )
      }
    })
  }

  const AsideMenu = menu => {
    const [key, setKey] = React.useState([])
    const [submenu, setsubmenu] = React.useState([])
    function click(ob) {
      console.log(ob)
      if (ob.key && navigate) {
        navigate(`${category}/${ob.key}`)
        setKey([ob.key])
      } else {
        console.log(ob)
      }
    }
    function menuClick(params) {
      setsubmenu(params)
    }
    const { mode } = useContext(ModeContext)
    return (
      <div className="sticky top-[60px] w-[250px] overflow-x-hidden h-[calc(100vh-60px)]">
        <Menu
          onClick={click}
          style={{
            width: "250",
            maxWidth: "250",
            height: "100%",
          }}
          theme={mode}
          onOpenChange={menuClick}
          selectedKeys={key}
          defaultOpenKeys={submenu}
          openKeys={submenu}
          defaultSelectedKeys={key}
          mode="inline"
          items={menu.children}
        />
      </div>
    )
  }
  console.log(menuData, "sss")
  return (
    <>
      <Seo title="纯钧" />

      <AppHeader />
      <div className=" dark:bg-[#1a1b1e] dark:text-gray-300   relative border dark:border-black justify-between bg-white text-[ #ccc] flex z-20 shadow-lg">
        {menuData.children &&
          menuData.children.length > 0 &&
          AsideMenu(menuData)}
        {children}
      </div>

      <AppFooter />
    </>
  )
}

export default AppContainer
