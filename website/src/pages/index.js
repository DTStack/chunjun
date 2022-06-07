import * as React from "react"
import AppBanner from "../components/AppBannner"
import AppCards from "../components/AppCards"
import AppFooter from "../components/AppFooter"
import AppShowcase from "../components/AppShowcase"
import AppMedium from "../components/AppMedium"
import Seo from "../components/seo"

const IndexPage = () => (
  <>
    <Seo title="纯钧" />
    <AppBanner />
    <AppCards />
    <AppMedium />
    <AppShowcase />
    <AppFooter />
  </>
)

export default IndexPage
