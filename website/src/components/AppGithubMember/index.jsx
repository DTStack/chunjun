import React from 'react'
import members from '../../../githubInfo/members.json'

const AppMembers = () => {
  return (
    <>
      <section className="w-[80%] m-auto my-[50px]">
        <p className="mb-[20px] text-[27px]">
          <span className="border-b-2 pb-[5px] text-[22px] font-bold  text-gray-700 border-gray-300 inline-block">
            贡献者
          </span>
        </p>
        <div className="flex flex-wrap m-[-7px]">
          {members.map(item => {
            return (
              <div key={item.id} className="border border-gray-200 flex items-center justify-center m-[7px] hover:bg-gray-50">
                <img
                  width={40}
                  height={40}
                  src={`https://github.com/${item.login}.png?size=40`}
                  alt=""
                />{' '}
                <a href={item.html_url} target="blank">
                  <span className="p-[5px]  text-gray-500"> {item.login} </span>
                </a>
              </div>
            )
          })}
        </div>
      </section>
    </>
  )
}

export default AppMembers
