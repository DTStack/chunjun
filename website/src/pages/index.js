import * as React from "react"
import AppHeader from "../components/AppHeader"
import AppBanner from "../components/AppBannner"
import AppFooter from "../components/AppFooter"
import AppCards from "../components/AppCards"
import AppMedium from "../components/AppMedium"
import AppShowCase from "../components/AppShowCase"
import Seo from "../components/seo"
import "aos/dist/aos.css"
import AppCustomers from "../components/AppCustomers"
const IndexPage = () => (
  <>
    <Seo title="纯钧" />
    <section className="dark:bg-[#1a1b1e]">
      <AppHeader />
      <AppBanner />

      <AppCards />
      <AppMedium />
      <AppShowCase />
      <AppCustomers />

      <AppFooter />
    </section>
  </>
)

export default IndexPage
