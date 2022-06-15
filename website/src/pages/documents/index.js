import * as React from "react"
import { graphql, Link, navigate, useStaticQuery } from "gatsby"

import AppHeader from "../../components/AppHeader"
import MenuLayout from "../../components/documentsMenu/menu"
import { buildMenu, getFileArr } from "../../util"
const IndexPage = props => {
  //nodes 是文档list的相关信息, 文档的详细路由是  /documents/{name}

  const menuData = buildMenu(props.data.allFile.edges.map(item => item.node))
  const fileList = getFileArr(menuData.children)
  if (fileList[0]) navigate(`/documents/${fileList[0].data.id}`)
  return (
    <>
      <h1 className="md__title">介绍</h1>
    </>
  )
}

export const query = graphql`
  query ($id: String) {
    markdownRemark(id: { eq: $id }) {
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

export default IndexPage
