import React, { useEffect } from "react"
import Aos from "aos"
import { Text } from "@mantine/core"

const AppCards = () => {
  useEffect(() => {
    Aos.init({
      duration: 1000,
    })
  }, [])

  return (
    <section className="w-full space-y-6">
      <section data-aos="zoom-in" className="w-full">
        <div className="text-center lg:text-3xl text-2xl font-bold md:w-3/5 w-4/5 m-auto uppercase font-mono">
          <span className="from-indigo-400 bg-gradient-to-r via-blue-500 to-pink-500 bg-clip-text text-transparent">sources&nbsp;&&nbsp;</span>
          <span className="from-green-400 bg-gradient-to-r via-blue-500 to-pink-500 bg-clip-text text-transparent">think</span>
        </div>
        <img className=" md:w-3/5 w-4/5 m-auto bg-gray-100" src={require("../../assets/img/link.jpg").default} alt="" />
      </section>
      <section data-aos="zoom-in" className="w-full">
        <h1 className="text-center lg:text-3xl text-2xl font-bold font-mono">
          ChunJun 纯钧 <span className="from-yellow-400 bg-gradient-to-r via-red-500 to-pink-500 bg-clip-text text-transparent">核心特性</span>
        </h1>
        <div className="grid md:grid-cols-3 grid-cols-2 md:gap-y-6 gap-y-4 justify-items-center lg:p-6 p-4">
          <div className="w-full flex flex-col items-center">
            <img className="card" src={require("../../assets/svg/hero-6.svg").default} alt="" />
            <Text className="card-text">基于json、sql 构建任务</Text>
          </div>
          <div className="w-full flex flex-col items-center">
            <img className="card" src={require("../../assets/svg/hero-2.svg").default} alt="" />
            <Text className="card-text">支持多种异构数据源之间数据传输</Text>
          </div>
          <div className="w-full flex flex-col items-center">
            <img className="card" src={require("../../assets/svg/hero-4.svg").default} alt="" />
            <Text className="card-text">支持断点续传、增量同步</Text>
          </div>
          <div className="w-full flex flex-col items-center">
            <img className="card" src={require("../../assets/svg/hero-1.svg").default} alt="" />
            <Text className="card-text">支持任务脏数据存储管理</Text>
          </div>
          <div className="w-full flex flex-col items-center">
            <img className="card" src={require("../../assets/svg/hero-3.svg").default} alt="" />
            <Text className="card-text">支持Schema同步</Text>
          </div>
          <div className="w-full flex flex-col items-center">
            <img className="card" src={require("../../assets/svg/hero-5.svg").default} alt="" />
            <Text className="card-text">支持RDBS数据源实时采集</Text>
          </div>
        </div>
      </section>
    </section>
  )
}

export default AppCards
