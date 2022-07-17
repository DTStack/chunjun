import React, { useEffect } from 'react'
import members from '../../../githubInfo/members.json'
import Aos from 'aos'

const AppShowCase = () => {
  useEffect(() => {
    Aos.init({
      duration: 1000
    })
  }, [])
  return <section className='md:p-8 p-4'>
    <h1 data-aos="zoom-in" className="font-mono mb-8 md:text-3xl text-2xl capitalize text-center font-bold from-green-400 bg-gradient-to-r  to-purple-600 bg-clip-text text-transparent">Become a contributor to Chunjun</h1>
    <div className="grid md:grid-cols-6 gap-2 grid-cols-2">
      {members.map((item, index) => <div data-aos="fade-up" data-aos-delay={index * 50} key={item.id} className="border shadow-sm border-gray-200 flex items-center justify-between m-sm hover:bg-gray-50 hover:scale-110 hover:shadow-md transition-all duration-100">
        <img
          width={40}
          height={40}
          src={`https://github.com/${item.login}.png?size=40`}
          alt=""
        />{' '}
        <a href={item.html_url} target="blank" className='flex-1 text-center'>
          <span className="font-mono text-gray-600 text-sm"> {item.login} </span>
        </a>
      </div>)}
    </div>
  </section>
}

export default AppShowCase
