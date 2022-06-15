import * as React from "react"
import { graphql } from "gatsby"
import ReactJson from "react-json-view"
import "./index.scss"
const BlogPost = props => {
  console.log(props.data, "mk")
  let json = {}
  try {
    json = JSON.parse(props.data.jsonContent.content)
  } catch {}
  return (
    <>
      <div className="json-pre">
        <ReactJson src={json} />
      </div>
      {/* <div className="markdown-body">
        <h1 className="md__title"> {title} </h1>
        <h3 className="md__tag">
          最后修改于 : {new Date(modifiedTime).toLocaleDateString()}{" "}
        </h3>
        <div dangerouslySetInnerHTML={{ __html: html }} />
      </div> */}
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
