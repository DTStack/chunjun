import React from "react"
import "./index.scss"
import AppHeader from "../AppHeader"

const AppBanner = () => {
  return (
    <div className="bg">
      <AppHeader />
      <section className="container">
        <div className="container__wrapper flex-padding">
          <h1 className="container__wrapper--title">
            <span className="container__wrapper--subtitle">ChunJun 纯钧</span>
            <p>一个基于Flink的批流统一的数据同步工具</p>
          </h1>
          <div className="container__wrapper--buttons">
            <button className="btn btn__blue btn__large">github</button>
            <button className="btn btn__white btn__large">联系我们</button>
          </div>
        </div>
      </section>
    </div>
  )
}

export default AppBanner
