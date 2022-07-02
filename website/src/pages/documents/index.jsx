import React from "react"
import { graphql, navigate } from "gatsby"
import { Skeleton } from "@mantine/core"

import { buildMenu, getFileArr } from "../../util"
const IndexPage = props => {
  const menuData = buildMenu(props.data.allFile.edges.map(item => item.node))
  const fileList = getFileArr(menuData.children)
  if (fileList[0] && navigate) navigate(`/documents/${fileList.find(page => page.name === "快速开始").data.id}`)
  return <Skeleton className="h-screen" visible />
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

export default IndexPage
