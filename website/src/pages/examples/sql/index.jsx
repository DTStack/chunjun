import React from "react"
import { graphql, navigate } from "gatsby"
import { buildMenu, getFileArr } from "../../../util"
import { Skeleton } from "@mantine/core"

const IndexPage = props => {
  const menuData = buildMenu(props.data.allFile.edges.map(item => item.node))
  console.log(props.data.allFile.edges.map(item => item.node));
  const fileList = getFileArr(menuData.children)
  
  return <Skeleton className="h-screen" visible />
}

export const query = graphql`
  query ($id: String) {
    jsonContent(id: { eq: $id }) {
      id
      content
    }
    allFile(filter: { sourceInstanceName: { eq: "examples" }, extension: { eq: "sql" }, ctime: {} }) {
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
