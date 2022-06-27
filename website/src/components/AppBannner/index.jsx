import React from "react"
import AppHeader from "../AppHeader"
import { Link } from "gatsby"

const AppBanner = () => {
  return (
    <section className="min-w-screen h-[650px] bg-hero-pattern md:bg-top relative bg-center bg-no-repeat bg-cover hero">
      <AppHeader />
      <section className="flex items-center md:flex-row flex-col-reverse mt-[80px]">
        <div className="flex-1 flex flex-col flex-padding">
          <h1 className="text-white text-3xl flex flex-col mb-[48px]">
            <span className="inline-block mb-[20px] md:text-4xl text-2xl text-center md:text-left">ChunJun 纯钧</span>
            <p className="inline-block font-mono md:text-xl text-base">
              基于Flink之上提供稳定，高效，易用的
              <br />
              批流一体的数据集成工具
            </p>
          </h1>
          <div className="mt-[30px] md:text-left text-center">
            <a className="btn btn__blue btn__large mr-2" target="_blank" href="https://github.com/DTStack/chunjun">
              github
            </a>
            <Link to="/documents/7e3fa940-adf2-5eb8-bfb2-9252eb015e73" className="btn btn__white btn__large">
              快速开始
            </Link>
          </div>
        </div>
      </section>
    </section>
  )
}

export default AppBanner
