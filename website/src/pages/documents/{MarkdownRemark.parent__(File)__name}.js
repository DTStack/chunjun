import * as React from "react"
import { graphql } from "gatsby"
import MenuLayout from "../../components/documentsMenu/menu"
import AppHeader from "../../components/AppHeader"
import "./index.scss"
import "./md.css"
import { Data } from "@icon-park/react"
const BlogPost = props => {
  const html = props.data.markdownRemark.html
  const data = props.data.markdownRemark
  console.log(props.data, "mk")
  const title = data.parent.name
  const modifiedTime = data.parent.modifiedTime
  return (
    <>
      <div className="markdown-body">
        <h1 className="md__title"> {title} </h1>
        <h3 className="md__tag">
          最后修改于 : {new Date(modifiedTime).toLocaleDateString()}{" "}
        </h3>
        <div dangerouslySetInnerHTML={{ __html: html }} />
      </div>
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
  }
`

export default BlogPost
