import React from "react"
import { graphql } from "gatsby"
import ReactJson from "react-json-view"

const BlogPost = props => {
  let json = {}
  try {
    json = JSON.parse(props.data.jsonContent.content)
  } catch {}

  return (
    <section className="w-full 2xl:flex 2xl:justify-center 2xl:items-center 2xl:text-2xl text-base">
      <ReactJson displayObjectSize={false} src={json} />
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
