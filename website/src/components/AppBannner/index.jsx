import React from "react"
import { Github } from "@icon-park/react"
import Link from "../Link"

const AppBanner = () => {
  return (
    <section className="relative hero bg-block2 dark:bg-[#1a1e1b] dark:text-[#797a7d] dark:bg-none  bg-center bg-no-repeat bg-contain flex items-center">
      <div className="px-4 py-8 flex flex-col items-center w-full">
        <p className="text-4xl lg:text-6xl xl:text-8xl text-center mb-8 inline-block">
          <span className="inline-block bg-gradient-to-r from-yellow-400 via-red-500 to-pink-500 bg-clip-text text-transparent">
            ChunJun
          </span>{" "}
          纯钧
        </p>
        <p className="inline-block xl:text-4xl font-mono text-xl text-center mb-6 font-bold">
          基于Flink之上提供稳定，高效，易用的 批流一体的数据集成工具
        </p>
        <div className="flex items-center">
          <a
            className="btn btn__black btn__large mr-4 text-base flex items-center xl:text-2xl"
            rel="noreferrer"
            target="_blank"
            href="https://github.com/DTStack/chunjun"
          >
            <Github color="#fff" className="mr-2 text-base xl:text-2xl" />
            github
          </a>
          <Link
            to="/documents"
            className="btn btn__blue btn__large text-base xl:text-2xl"
          >
            快速开始
          </Link>
        </div>
      </div>
    </section>
  )
}

export default AppBanner
