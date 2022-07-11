import React, { useEffect } from 'react'
import Aos from 'aos'

const AppMedium = () => {
  useEffect(() => {
    Aos.init({
      duration: 1000
    })
  }, [])
  // data-aos="fade-up"
  return <section className="w-full dark:bg-[#1a1b1e] bg-[#dff9fb] lg:p-6 p-4">
    <h1 className="font-mono mb-8 lg:text-3xl text-4xl capitalize text-center font-bold from-green-400 bg-gradient-to-r  to-purple-600 bg-clip-text text-transparent">easy to learn. easy to use.</h1>
    <div className="w-full flex items-center flex-col md:flex-row space-x-6">
      <div className='h-[400px] w-1/3 bg-black rounded-md p-6'>
        <pre className='lang-java h-3/5 w-full rounded-md'>
          <code className='h-full w-full'>

          </code>
        </pre>
      </div>
      <div className="flex-1 flex flex-col h-[400px] space-y-4 rounded-md">
        <div className='h-1/2 w-full rounded-md bg-white'></div>
        <div className='h-1/2 w-full rounded-md bg-white'></div>
      </div>
    </div>
  </section>
}

export default AppMedium
