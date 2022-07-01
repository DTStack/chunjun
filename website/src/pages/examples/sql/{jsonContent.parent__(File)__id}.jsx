import React from "react"
import { graphql } from "gatsby"
import ReactJson from "react-json-view"
import hljs from 'highlight.js';

const BlogPost = props => {
  
  let sql =  ''
  try {
    sql =  (props.data.jsonContent.content)
  } catch {}
  React.useLayoutEffect(()=>{
    hljs.highlightAll()
  },[])
  return (
    <section className="json">
      <pre><code class="language-html">
      {sql}
        </code></pre>
       
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
