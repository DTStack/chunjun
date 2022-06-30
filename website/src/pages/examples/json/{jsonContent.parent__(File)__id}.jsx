import React from "react"
import { graphql } from "gatsby"
import ReactJson from "react-json-view"

const BlogPost = props => {
  let json = {}
  try {
    json = JSON.parse(props.data.jsonContent.content)
  } catch {}
  return (
    <section className="json">
      <ReactJson src={json} />
    </section>
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
