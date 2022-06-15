import * as React from "react"
import AppBanner from "../components/AppBannner"
import AppCards from "../components/AppCards"
import AppFooter from "../components/AppFooter"
import AppShowcase from "../components/AppShowcase"
import AppMedium from "../components/AppMedium"
import Seo from "../components/seo"
import "aos/dist/aos.css"
import Flow from "../components/AppInteract"
const IndexPage = () => (
  <>
    <Seo title="纯钧" />
    <AppBanner />
    <AppCards />

    <AppMedium />
    <div
      className="w-full bg-white relative my-3"
      style={{ height: `500px`, overflowX: "auto" }}
    >
      <Flow></Flow>
    </div>
    <AppShowcase />

    <AppFooter />
  </>
)

export default IndexPage
