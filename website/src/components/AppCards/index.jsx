import React, { useEffect } from 'react'
import Aos from 'aos'
import { Text } from '@mantine/core'

const AppCards = () => {
  useEffect(() => {
    Aos.init({
      duration: 1000
    })
  }, [])

  return (
    <section className="w-full space-y-6 dark:bg-[#1a1b1e] bg-wave bg-contain bg-top">
      <section data-aos="zoom-in" className="w-full dark:text-[#797a7d]">
        <h1 data-aos="fade-down" className="text-center lg:text-3xl text-4xl font-bold font-mono dark:text-[#797a7d] from-blue-400 bg-gradient-to-r via-indigo-500  to-purple-600 bg-clip-text text-transparent capitalize">Core features
        </h1>
        <div className="grid md:grid-cols-3 grid-cols-2 md:gap-y-6 gap-8 justify-items-center lg:p-6 p-4">
          <div className="w-full flex flex-col items-center space-y-6">
            <img className="card" src={require('../../assets/svg/code.svg').default} alt="code" />
            <Text className="card-text">基于json、sql 构建任务</Text>
          </div>
          <div className="w-full flex flex-col items-center space-y-6">
            <img className="card" src={require('../../assets/svg/trans.svg').default} alt="trans" />
            <Text className="card-text">支持多种异构数据源之间数据传输</Text>
          </div>
          <div className="w-full flex flex-col items-center space-y-6">
            <img className="card" src={require('../../assets/svg/sync.svg').default} alt="sync" />
            <Text className="card-text">支持断点续传、增量同步</Text>
          </div>
          <div className="w-full flex flex-col items-center space-y-6">
            <img className="card" src={require('../../assets/svg/hcs_sgw.svg').default} alt="dirty-data" />
            <Text className="card-text">支持任务脏数据存储管理</Text>
          </div>
          <div className="w-full flex flex-col items-center space-y-6">
            <img className="card" src={require('../../assets/svg/datav.svg').default} alt="schema" />
            <Text className="card-text">支持Schema同步</Text>
          </div>
          <div className="w-full flex flex-col items-center space-y-6">
            <img className="card" src={require('../../assets/svg/collect.svg').default} alt="collect" />
            <Text className="card-text">支持RDBS数据源实时采集</Text>
          </div>
        </div>
      </section>
    </section>
  )
}

export default AppCards
