import * as React from "react"
import { graphql } from "gatsby"
import ReactJson from "react-json-view"
import "./index.scss"
const BlogPost = props => {
  let json = {}
  try {
    json = JSON.parse(props.data.jsonContent.content)
  } catch {}
  return (
    <>
      <div className="json-pre">
        <ReactJson src={json} />
      </div>
    </>
  )
}

export const query = graphql`
  query ($id: String) {
    jsonContent(id: { eq: $id }) {
      id
      content
    }
  }
`

export default BlogPost
