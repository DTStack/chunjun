import Aos from "aos"
import React, { useEffect } from "react"

const AppMedium = () => {
  useEffect(() => {
    Aos.init({
      duration: 1000,
    })
  }, [])

  return (
    <section data-aos="zoom-out" className="w-full">
      <h1 className="text-center lg:text-4xl text-2xl font-bold">加入纯钧</h1>
      <div className="flex flex-col justify-center items-center rounded-md shadow-sm py-8">
        <a className="inline-block w-full mb-6 " href="https://github.com/DTStack/chunjun/graphs/contributors">
          <img className="h-48 w-full" alt="github" src="https://contrib.rocks/image?repo=DTStack/chunjun" />
        </a>
        <a className="btn btn__large btn__blue" rel="noreferrer" target="_blank" href="https://github.com/DTStack/chunjun">
          现在加入
        </a>
      </div>
    </section>
  )
}

export default AppMedium
