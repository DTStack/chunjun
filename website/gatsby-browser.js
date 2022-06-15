/**
 * Implement Gatsby's Browser APIs in this file.
 *
 * See: https://www.gatsbyjs.com/docs/browser-apis/
 */
require("./src/styles/global.css")
const React = require("react")
const { default: AppHeader } = require("./src/components/AppHeader")
const { default: AppHeaderWhite } = require("./src/components/AppHeaderWhite")

const Layout = require("./src/components/documentsMenu/menu").default
const JsonLayout = require("./src/components/jsonMenu/menu").default
const SpaceLayout = require("./src/components/space/spaceLayout").default
const { default: AppFooter } = require("./src/components/AppFooter")
// You can delete this file if you're not using it
import("./src/assets/sass/index.scss")
exports.wrapPageElement = ({ element, props }) => {
  // props provide same data to Layout as Page element will get
  // including location, data, etc - you don't need to pass it

  return (
    <>
      <div className=" ">
        {element.key.includes("documents") ? (
          <>
            <div style={{ minHeight: "100vh", backgroundColor: "#fff" }}>
              <AppHeaderWhite></AppHeaderWhite>
              <Layout {...props}>{element}</Layout>
            </div>
          </>
        ) : element.key.includes("examples") ? (
          <div style={{ minHeight: "100vh", backgroundColor: "#fff" }}>
            <AppHeaderWhite></AppHeaderWhite>
            <JsonLayout {...props}>{element}</JsonLayout>
          </div>
        ) : (
          <SpaceLayout {...props}>{element}</SpaceLayout>
        )}
      </div>
    </>
  )
}
