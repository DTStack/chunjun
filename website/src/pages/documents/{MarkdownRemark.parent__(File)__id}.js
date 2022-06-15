import * as React from "react"
import { graphql, navigate, useStaticQuery } from "gatsby"
import "./index.scss"
import "./md.css"
import { buildMenu, getFileArr } from "../../util"
import "../../font/iconfont.css"
import AppFooter from "../../components/AppFooter"
const BlogPost = props => {
  // const menuData = buildMenu(ldata.allFile.edges.map(item => item.node))

  const menuData = buildMenu(props.data.allFile.edges.map(item => item.node))
  const fileList = getFileArr(menuData.children)
  const html = props.data.markdownRemark.html
  const tableOfContents = props.data.markdownRemark.tableOfContents
  const data = props.data.markdownRemark
  const title = data.parent.name
  const modifiedTime = data.parent.modifiedTime

  // const state = props.location.state
  let location = window.location.pathname.split("/").pop()
  let fileIndex = fileList.map(item => item.data.id).indexOf(location)

  const [preName, setPre] = React.useState("（无）")
  const [nextName, setNext] = React.useState("（无）")

  React.useEffect(() => {
    if (fileIndex > 0) {
      setPre(fileList[fileIndex - 1].name)
    }
    if (fileIndex != fileList.length - 1) {
      setNext(fileList[fileIndex + 1].name)
    }
  }, [])

  function goPre() {
    if (fileIndex === 0) return

    let target = fileList[fileIndex - 1]
    navigate(`/documents/${target.data.id}`, {
      state: {
        fileIndex: fileIndex - 1,
        fileList: fileList,
      },
    })
  }
  function goNext() {
    if (fileIndex + 1 == fileList.length) return
    let target = fileList[fileIndex + 1]
    navigate(`/documents/${target.data.id}`, {
      state: {
        fileIndex: fileIndex + 1,
        fileList: fileList,
      },
    })
  }

  return (
    <>
      <div className="p-0 dark:bg-black dark:text-white relative">
        <div className="dark:bg-black flex p-0">
          <section
            className="dark-markdown-body dark:bg-black px-7 document-content flex-1 mr-3 pr-3"
            style={{ minHeight: "50vh" }}
          >
            <h1 className="md__title"> {title} </h1>
            <h3 className="text-sm text-blue-400 text-left">
              更新于 : {new Date(modifiedTime).toLocaleDateString()}{" "}
            </h3>

            <div dangerouslySetInnerHTML={{ __html: html }} />
            <div className="btn-group ">
              <button
                className="go-btn pre-btn border flex justify-between items-center border-gray-200 hover:border-blue-400  text-base shadow-sm hover:shadow"
                onClick={goPre}
              >
                <i className="iconfont text-2xl icon-left-line"></i>
                <div>
                  <p className="top-text">上一篇</p>
                  <p className="bo-text">
                    <span className="font-bold text-3xl">{preName}</span>
                  </p>
                </div>
              </button>
              <button
                className="go-btn pre-btn border flex justify-between items-center border-gray-200 hover:border-blue-400  text-base shadow-sm hover:shadow"
                onClick={goNext}
              >
                <div>
                  <p className="top-text">下一篇</p>
                  <p className="bo-text">
                    <span className=" text-3xl font-bold">{nextName}</span>
                  </p>
                </div>
                <i className="iconfont text-2xl icon-right-line"></i>
              </button>
            </div>
            <AppFooter></AppFooter>
          </section>
          <section
            style={{ width: "250px" }}
            className=" flex-grow-0 p-0  border-l border-gray-200 flex-shrink-0 pl-5  table-of-content"
          >
            <div
              className="table-of-content"
              dangerouslySetInnerHTML={{ __html: tableOfContents }}
            />
          </section>
        </div>
      </div>
    </>
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
`

export default BlogPost
