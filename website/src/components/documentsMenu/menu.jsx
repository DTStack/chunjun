import * as React from "react"
import { graphql, Link, navigate, useStaticQuery } from "gatsby"
import "./index.scss"

/**
 * @description 目录解析
 * @param {nodes} 文件信息
 * @reference
 */
function buildMenu(nodes) {
  let id = 0
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
        }
        rootRef.children.push(nextRef)
      }
      rootRef = nextRef
    }
    rootRef.children.push({
      type: "file",
      name: node.name,
      data: node,
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
      })
    } else {
      linkToRoot(structrue, node)
    }
  }
  console.log(root)
  return root
}

const MenuItem = ({ item, listStyle, re }) => {
  const none = {
    display: "none",
  }
  const block = {
    display: "block",
  }
  function toggle(link) {
    if (link.type === "file") {
      navigate(`/documents/${link.name}`)
      return
    }
    showMap[link.id] = !showMap[link.id]
    setShowMap(showMap)
    setT(!tt)
    console.log(link, showMap, "toggle")
  }
  const [showMap, setShowMap] = React.useState({})
  const [tt, setT] = React.useState(false)
  console.log(item, "@")
  React.useEffect(() => {
    console.log("helo")
  }, [tt, re])
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
                    showMap[link.id]
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
                ></MenuItem>
              </div>
            </>
          )
        })}
    </>
  )
}

const MenuLayout = ({ children }) => {
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
  const nodes = data.allFile.edges
  return (
    <>
      <div className="menu__layout">
        <section className="menu">{MenuItem({ item: menuData })}</section>
        <section style={{ flex: 1, padding: "10px" }}>{children}</section>
      </div>
    </>
  )
}

export default MenuLayout
