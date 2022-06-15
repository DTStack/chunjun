import * as React from "react"
import { graphql, Link, navigate, useStaticQuery } from "gatsby"
import "./index.scss"

export const State = React.createContext({})

/**
 * @description 目录解析
 * @param {nodes} 文件信息
 * @reference
 */
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
  console.log(root)
  return root
}

function getFileArr(list) {
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

const MenuLayout = ({ children }) => {
  let location = window.location.pathname.split("/").pop()
  //nodes 是文档list的相关信息, 文档的详细路由是  /documents/{name}
  const data = useStaticQuery(graphql`
    query MyQuery {
      allFile(
        filter: {
          sourceInstanceName: { eq: "docs" }
          extension: { eq: "md" }
          ctime: {}
        }
      ) {
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

  const menuData = buildMenu(data.allFile.edges.map(item => item.node))

  const fileList = getFileArr(menuData.children)

  const [bool, setBool] = React.useState(false)
  const [active, setActive] = React.useState(null)
  const [showMap, setShowMap] = React.useState({})
  const tFile = fileList.find(item => item.data.id === location)

  //遍历目录  ， 依据url展开目录（包括所有母目录）
  function traverse(node) {
    if (!node) return
    if (Number(node.id) > 0) {
      showMap[node.id] = true
    }

    if (node.parent && node.parent.type === "dir") {
      traverse(node.parent)
    }
  }

  if (location !== active) {
    setActive(location)
    traverse(tFile)
    setShowMap(showMap)
    setBool(!bool)
  }

  React.useEffect(() => {}, [bool])

  const MenuItem = ({ item, listStyle, re, active, setActive }) => {
    const none = {
      display: "none",
    }
    const block = {
      display: "block",
    }
    function toggle(link) {
      if (link.type === "file") {
        let index = -1
        for (let i = 0; i < fileList.length; i++) {
          let item = fileList[i]
          if (item.data.id === link.data.id) {
            index = i
            break
          }
        }
        navigate(`/documents/${link.data.id}`)
        return
      } else {
        showMap[link.id] = !showMap[link.id]
        setShowMap(showMap)
        setT(!tt)
      }
    }

    const [tt, setT] = React.useState(false)

    React.useEffect(() => {}, [tt, re])
    return (
      <>
        {item.children &&
          item.children.map(link => {
            return (
              <>
                <div style={listStyle}>
                  <li
                    onClick={() => {
                      toggle(link)
                    }}
                    className={
                      (showMap[link.id] && false) || active === link?.data?.id
                        ? "menu__item menu__item-active"
                        : "menu__item"
                    }
                    key={link.name}
                  >
                    <div className="toggle">
                      {link.name}
                      {link.type === "dir" ? (
                        <span
                          className={
                            showMap[link.id]
                              ? "toggle__dir rotate"
                              : "toggle__dir"
                          }
                        >
                          {" "}
                          &gt;{" "}
                        </span>
                      ) : null}
                    </div>
                  </li>
                  <MenuItem
                    re={tt}
                    listStyle={showMap[link.id] ? block : none}
                    item={link}
                    active={active}
                    setActive={setActive}
                  ></MenuItem>
                </div>
              </>
            )
          })}
      </>
    )
  }

  return (
    <>
      <State.Provider value={{ fileList }}>
        <div className="menu__layout">
          <section
            className="menu document-menu"
            style={{ height: "calc(100vh - 80px)", overflowY: "auto" }}
          >
            {MenuItem({ item: menuData, active: active, setActive })}
          </section>
          <section style={{ flex: 1 }}>{children}</section>
        </div>
      </State.Provider>
    </>
  )
}

export default MenuLayout
