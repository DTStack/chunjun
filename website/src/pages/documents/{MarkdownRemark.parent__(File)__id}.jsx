import React from "react"
import { graphql, navigate } from "gatsby"
import { buildMenu, getFileArr } from "../../util"
import "./index.scss"
import "./hljs.scss"
import { useLocation } from "@reach/router"
import { useSpring, animated } from "react-spring"
import { MenuUnfoldOutlined } from "@ant-design/icons"
import { BsArrowLeft, BsArrowRight } from "react-icons/bs"
import hljs from "highlight.js"
const BlogPost = props => {
  const menuData = buildMenu(props.data.allFile.edges.map(item => item.node))
  const fileList = getFileArr(menuData.children)
  const html = props.data.markdownRemark.html
  const tableOfContents = props.data.markdownRemark.tableOfContents.replace(new RegExp("<a", "g"), "<a permalink")
  console.log(tableOfContents)
  const location = useLocation().pathname.split("/").pop()
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
  React.useLayoutEffect(() => {
    if (window) window.scrollTo(0, 0)
    hljs.highlightAll()
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
  const aprops = useSpring({
    to: { opacity: 1, left: 0 },
    from: { opacity: 0, left: 100 },
  })

  return (
    <>
      <animated.div style={{ "max-width": "800px", ...aprops }} className="relative  md:w-[calc(100%-600px)] px-4 pb-3 w-full">
        <div className="w-full pt-4 container-content" dangerouslySetInnerHTML={{ __html: html }} />

        <div className="w-full flex-wrap flex items-cen ter justify-between">
          <button className="ring-1 ring-gray-50  hover:bg-[#f8f9fa]  dark:ring-gray-600  w-full md:w-[45%] mb-1    hover:shadow-none hover:border border dark:hover:bg-black  shadow-sm text-sm flex items-center dark:border-[#1a1b1e]  rounded-sm text-gray-600 justify-between    py-4 px-8" onClick={goPre}>
            <BsArrowLeft theme="outline" size="24" fill="#333" />
            <span className="text-right px-[5px]">
              <div className=" text-[20px] mb-3 text-gray-600">上一篇</div>
              <div className="text-[#acb2b7]">{preName}</div>
            </span>
          </button>
          <button className="ring-1 ring-gray-50  hover:bg-[#f8f9fa]  dark:ring-gray-600  w-full md:w-[45%] mb-1    hover:shadow-none hover:border border dark:hover:bg-black  shadow-sm text-sm flex items-center dark:border-[#1a1b1e]  rounded-sm text-gray-600 justify-between    py-4 px-8" onClick={goNext}>
            <span className="text-right px-[5px]">
              <div className=" text-[20px] mb-3 text-gray-600">下一篇</div>
              <div className="text-[#acb2b7]">{nextName}</div>
            </span>
            <BsArrowRight theme="outline" size="24" fill="#333" />
          </button>
        </div>
      </animated.div>

      <aside style={{ height: "calc(100vh - 90px)" }} className="hidden md:inline-block  w-[300px]  flex-shrink-0 text-sm sticky top-[90px] bottom-[-200px] list-none">
        <div className="flex items-center text-base">
          <MenuUnfoldOutlined /> <span className="mx-3">目录</span>
        </div>
        <div className=" right-0  container-content container-content-toc" dangerouslySetInnerHTML={{ __html: tableOfContents }} />
      </aside>
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
