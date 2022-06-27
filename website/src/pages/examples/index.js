import * as React from "react"
import { graphql, navigate } from "gatsby"

import { buildMenu, getFileArr } from "../../util"
const IndexPage = props => {
  //nodes 是文档list的相关信息, 文档的详细路由是  /documents/{name}

  const menuData = buildMenu(props.data.allFile.edges.map(item => item.node))
  const fileList = getFileArr(menuData.children)
  if (fileList[0]) navigate(`/examples/${fileList[0].data.id}`)
  return (
    <>
      <h1 className="md__title">Loading....</h1>
    </>
  )
}

export const query = graphql`
  query ($id: String) {
    jsonContent(id: { eq: $id }) {
      id
      content
    }
    allFile(filter: { sourceInstanceName: { eq: "examples" }, extension: { eq: "json" }, ctime: {} }) {
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
