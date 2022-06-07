import * as React from "react"
import { graphql, Link } from "gatsby"

import AppHeader from "../../components/AppHeader"
import MenuLayout from "../../components/documentsMenu/menu"
const IndexPage = ({ data }) => {
  //nodes 是文档list的相关信息, 文档的详细路由是  /documents/{name}

  return (
    <>
      <h1 className="md__title">介绍</h1>
    </>
  )
}

export default IndexPage
