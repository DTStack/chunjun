import React from "react"
import { graphql, navigate } from "gatsby"
import { buildMenu, getFileArr } from "../../util"
import { Left, Right } from "@icon-park/react"
import "./index.scss"

const BlogPost = props => {
  const menuData = buildMenu(props.data.allFile.edges.map(item => item.node))
  const fileList = getFileArr(menuData.children)
  const html = props.data.markdownRemark.html
  const tableOfContents = props.data.markdownRemark.tableOfContents

  const location = window.location.pathname.split("/").pop()
  const fileIndex = fileList.map(item => item.data.id).indexOf(location)

  const [preName, setPre] = React.useState("（无）")
  const [nextName, setNext] = React.useState("（无）")

  React.useEffect(() => {
    if (fileIndex > 0) {
      setPre(fileList[fileIndex - 1].name)
    }
    if (fileIndex !== fileList.length - 1) {
      setNext(fileList[fileIndex + 1].name)
    }
  }, [])

  function goPre() {
    if (fileIndex === 0) return

    const target = fileList[fileIndex - 1]
    navigate(`/documents/${target.data.id}`, {
      state: {
        fileIndex: fileIndex - 1,
        fileList: fileList,
      },
    })
  }
  function goNext() {
    if (fileIndex + 1 === fileList.length) return
    const target = fileList[fileIndex + 1]
    navigate(`/documents/${target.data.id}`, {
      state: {
        fileIndex: fileIndex + 1,
        fileList: fileList,
      },
    })
  }

  return (
    <section className="container px-4 w-full">
      <div className="flex">
        <div className="container-wrapper md:w-2/3 2xl:w-4/5" dangerouslySetInnerHTML={{ __html: html }} />
        <aside className="text-sm sticky list-none">
          <div dangerouslySetInnerHTML={{ __html: tableOfContents }} />
        </aside>
      </div>
      <div className="w-2/3 flex items-center justify-between">
        <button className="ring-1 ring-gray-50 shadow-md text-sm flex items-center rounded-sm text-gray-600 w-[200px] py-4" onClick={goPre}>
          <Left theme="outline" size="24" fill="#333" />
          <span className="text-left px-[5px]">
            <div className="m-0 text-gray-600">上一篇:</div>
            <div className="text-black">{preName}</div>
          </span>
        </button>
        <button className="ring-1 ring-gray-50 shadow-md text-sm flex items-center rounded-sm text-gray-600 w-[200px] py-4" onClick={goNext}>
          <span className="text-right px-[5px]">
            <div className="m-0 text-gray-600">下一篇:</div>
            <div className="text-black">{nextName}</div>
          </span>
          <Right theme="outline" size="24" fill="#333" />
        </button>
      </div>
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
