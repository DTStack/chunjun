import React, { useEffect } from "react"
import Aos from "aos"

const AppCards = () => {
  useEffect(() => {
    Aos.init({
      duration: 1000,
    })
  }, [])

  return (
    <>
      <section data-aos="zoom-in" className="flex-padding py-[36px] w-full mb-[48px]">
        <h1 className="text-center mb-[54px] text-xl">ChunJun 纯钧 核心特性</h1>
        <div className="grid md:grid-cols-3 grid-cols-2 md:gap-y-[52px] gap-y-4 justify-items-center">
          <div className="md:w-[256px] w-full flex flex-col items-center">
            <img className=" mb-[15px] md:w-[80px] w-[96px]" src={require("../../assets/svg/hero-6.svg").default} alt="" />
            <p className="text-center text-gray-400">基于json、sql 构建任务</p>
          </div>
          <div className="md:w-[256px] w-full flex flex-col items-center">
            <img className=" mb-[15px] md:w-[80px] w-[96px]" src={require("../../assets/svg/hero-2.svg").default} alt="" />
            <p className="text-center text-gray-400">支持多种异构数据源之间数据传输</p>
          </div>
          <div className="md:w-[256px] w-full flex flex-col items-center">
            <img className=" mb-[15px] md:w-[80px] w-[96px]" src={require("../../assets/svg/hero-4.svg").default} alt="" />
            <p className="text-center text-gray-400">支持断点续传、增量同步</p>
          </div>
          <div className="md:w-[256px] w-full flex flex-col items-center">
            <img className=" mb-[15px] md:w-[80px] w-[96px]" src={require("../../assets/svg/hero-1.svg").default} alt="" />
            <p className="text-center text-gray-400">支持任务脏数据存储管理</p>
          </div>
          <div className="md:w-[256px] w-full flex flex-col items-center">
            <img className=" mb-[15px] md:w-[80px] w-[96px]" src={require("../../assets/svg/hero-3.svg").default} alt="" />
            <p className="text-center text-gray-400">支持Schema同步</p>
          </div>
          <div className="md:w-[256px] w-full flex flex-col items-center">
            <img className=" mb-[15px] md:w-[80px] w-[96px]" src={require("../../assets/svg/hero-5.svg").default} alt="" />
            <p className="text-center text-gray-400">支持RDBS数据源实时采集</p>
          </div>
        </div>
      </section>
    </>
  )
}

export default AppCards
