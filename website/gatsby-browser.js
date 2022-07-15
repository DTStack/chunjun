/**
 * Implement Gatsby's Browser APIs in this file.
 *
 * See: https://www.gatsbyjs.com/docs/browser-apis/
 */
require("./src/styles/global.css")
const React = require("react")
// const { default: AppHeader } = require("./src/components/AppHeader")

const Layout = require("./src/components/documentsMenu/menu").default
const JsonLayout = require("./src/components/jsonMenu/menu").default
const SqlLayout = require("./src/components/sqlMenu/menu").default
const SpaceLayout = require("./src/components/space/spaceLayout").default
// const AppFooter = require("./src/components/AppFooter").default

// You can delete this file if you're not using it
// import("./src/assets/sass/index.scss")
exports.wrapPageElement = ({ element, props }) => {
  // props provide same data to Layout as Page element will get
  // including location, data, etc - you don't need to pass it

  const flag = element.key.includes("documents") || element.key.includes("examples") || element.key.includes("download")
  console.log(element.key)
  return (
    <div>
      {flag ? (
        <>
          {element.key.includes("documents") && <Layout {...props}>{element}</Layout>}
          {element.key.includes("examples/json") && <JsonLayout {...props}>{element}</JsonLayout>}
          {element.key.includes("examples/sql") && <SqlLayout {...props}>{element}</SqlLayout>}
        </>
      ) : (
        <SpaceLayout {...props}>{element}</SpaceLayout>
      )}
    </div>
  )
}

exports.shouldUpdateScroll = ({ routerProps: { location } }) => {
  const { pathname } = location
  // list of routes for the scroll-to-top-hook
  const scrollToTopRoutes = [`/documents`, `/examples`]
  // if the new route is part of the list above, scroll to top (0, 0)
  if (scrollToTopRoutes.find(path => pathname.includes(path))) {
    document.querySelector(".main")?.scrollTo(0, 0)
  }

  return false
}
