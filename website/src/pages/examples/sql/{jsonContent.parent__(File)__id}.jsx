import React from "react"
import { graphql } from "gatsby"
import Highlight from "react-highlight"
import "./index.scss"
import { useSpring, animated } from "react-spring"
const BlogPost = props => {
  let sql = ""
  try {
    sql = props.data.jsonContent.content
  } catch {}
  const aprops = useSpring({
    to: { opacity: 1, left: 0 },
    from: { opacity: 0, left: 100 },
  })
  return (
    <animated.section
      style={aprops}
      className="w-full 2xl:flex 2xl:justify-center 2xl:items-center 2xl:text-2xl text-base"
    >
      <Highlight className="sql w-full overflow-x-hidden" language="sql">
        {sql}
      </Highlight>
    </animated.section>
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
