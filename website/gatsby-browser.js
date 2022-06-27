/**
 * Implement Gatsby's Browser APIs in this file.
 *
 * See: https://www.gatsbyjs.com/docs/browser-apis/
 */
require("./src/styles/global.css")
const React = require("react")
const { default: AppHeader } = require("./src/components/AppHeader")

const Layout = require("./src/components/documentsMenu/menu").default
const JsonLayout = require("./src/components/jsonMenu/menu").default
const SpaceLayout = require("./src/components/space/spaceLayout").default
// You can delete this file if you're not using it
// import("./src/assets/sass/index.scss")
exports.wrapPageElement = ({ element, props }) => {
  // props provide same data to Layout as Page element will get
  // including location, data, etc - you don't need to pass it

  const flag = element.key.includes("documents") || element.key.includes("examples") || element.key.includes("download")

  return (
    <div className="min-h-screen">
      {flag ? (
        <>
          <AppHeader color={true} />
          {element.key.includes("documents") && <Layout {...props}>{element}</Layout>}
          {element.key.includes("examples") && <JsonLayout {...props}>{element}</JsonLayout>}
        </>
      ) : (
        <>
          <SpaceLayout {...props}>{element}</SpaceLayout>
        </>
      )}
    </div>
  )
}
