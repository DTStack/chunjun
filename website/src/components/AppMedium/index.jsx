import Aos from "aos"
import React, { useEffect } from "react"

const AppMedium = () => {
  useEffect(() => {
    Aos.init({
      duration: 1000,
    })
  }, [])

  return (
    <section data-aos="zoom-in" className="w-full dark:bg-[#1a1b1e]">
      <h1 className="text-center lg:text-3xl text-2xl font-bold font-mono ">
        <span className="from-indigo-400 bg-gradient-to-r via-red-500 dark:text-[#797a7d] to-pink-500 bg-clip-text text-transparent">
          加入纯钧
        </span>
      </h1>
      <div className="flex flex-col justify-center items-center rounded-md shadow-sm p-8">
        <a
          className="inline-block w-full"
          href="https://github.com/DTStack/chunjun/graphs/contributors"
        >
          <img
            className="w-4/5 m-auto"
            alt="github"
            src="https://contrib.rocks/image?repo=DTStack/chunjun"
          />
        </a>
        <a
          className="btn btn__large btn__blue"
          rel="noreferrer"
          target="_blank"
          href="https://github.com/DTStack/chunjun"
        >
          现在加入
        </a>
      </div>
    </section>
  )
}

export default AppMedium
