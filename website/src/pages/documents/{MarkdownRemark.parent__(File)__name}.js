import * as React from "react"
import { graphql, navigate } from "gatsby"
import { buildMenu, getFileArr } from "../../util"
import { Left, Right } from "@icon-park/react"
import "./index.scss"

const BlogPost = props => {
  // const menuData = buildMenu(ldata.allFile.edges.map(item => item.node))

  const menuData = buildMenu(props.data.allFile.edges.map(item => item.node))
  const fileList = getFileArr(menuData.children)
  const html = props.data.markdownRemark.html
  const tableOfContents = props.data.markdownRemark.tableOfContents
  // const data = props.data.markdownRemark

  const location = window.location.pathname.split("/").pop()
  const fileIndex = fileList.map(item => item.data.id).indexOf(location)

  const [preName, setPre] = React.useState("无")
  const [nextName, setNext] = React.useState("无")

  React.useEffect(() => {
    if (fileIndex > 0) {
      setPre(fileList[fileIndex - 1].name)
    }
    if (fileIndex !== fileList.length - 1) {
      setNext(fileList[fileIndex + 1].name)
    }
  }, [])

  function goPre() {
    if (fileIndex === 0 || preName === "无") return

    let target = fileList[fileIndex - 1]
    navigate(`/documents/${target.data.id}`, {
      state: {
        fileIndex: fileIndex - 1,
        fileList: fileList,
      },
    })
  }
  function goNext() {
    if (fileIndex + 1 === fileList.length || nextName === "无") return
    let target = fileList[fileIndex + 1]
    navigate(`/documents/${target.data.id}`, {
      state: {
        fileIndex: fileIndex + 1,
        fileList: fileList,
      },
    })
  }

  return (
    <section className="container">
      <div>
        <div className="container-wrapper" dangerouslySetInnerHTML={{ __html: html }} />
        <div className="container-group">
          <button className="container-group-btn" onClick={goPre}>
            <Left theme="outline" size="24" fill="#333" />
            <span style={{ textAlign: "left", padding: "0 5px" }}>
              <p>上一篇:</p>
              {preName}
            </span>
          </button>
          <button className="container-group-btn" onClick={goNext}>
            <span style={{ textAlign: "right", padding: "0 5px" }}>
              <p>下一篇:</p>
              {nextName}
            </span>
            <Right theme="outline" size="24" fill="#333" />
          </button>
        </div>
      </div>
      <aside className="outline">
        <div className="outline-wrapper" dangerouslySetInnerHTML={{ __html: tableOfContents }} />
      </aside>
    </section>
  )
}

export const query = graphql`
  query ($id: String) {
    markdownRemark(id: { eq: $id }) {
      tableOfContents(maxDepth: 2)
      html
      id
      parent {
        ... on File {
          id
          name
          modifiedTime
          ino
        }
      }
    }
    allFile(filter: { sourceInstanceName: { eq: "docs" }, extension: { eq: "md" }, ctime: {} }) {
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
`

export default BlogPost
