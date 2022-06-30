const isNodeSupported = node => {
  return isTextFile(node) || isFileWithoutExtension(node)
}

const isTextFile = node => {
  return node.internal.type === "File" && (node.internal.mediaType === "application/json" || node.internal.mediaType.includes("sql"))
}

const isFileWithoutExtension = node => {
  return node.internal.type === "File" && node.internal.mediaType === "application/octet-stream" && !node.internal.extension
}

exports.onCreateNode = async ({ node, actions, loadNodeContent, createNodeId, createContentDigest }) => {
  if (!isNodeSupported(node)) {
    return
  }
  const { createNode, createParentChildLink } = actions
  const content = await loadNodeContent(node)
  const id = createNodeId(`${node.id} >>> jsonContent`)
  const plainTextNode = {
    id,
    children: [],
    content,
    parent: node.id,
    internal: {
      contentDigest: createContentDigest(content),
      type: "jsonContent",
    },
  }
  createNode(plainTextNode)
  createParentChildLink({
    parent: node,
    child: plainTextNode,
  })
}
