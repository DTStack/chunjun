import React, { useState } from "react"
import { Link } from "gatsby"
import { Burger } from "@mantine/core"
import AppDrawer from "../AppDrawer"
import { headerList } from "../../util"

const AppHeader = ({ color }) => {
  const [opened, setOpened] = useState(false)

  const links = headerList

  return (
    <header className={`w-full h-[64px] flex shadow-sm items-center ${color ? "text-black" : "text-white flex-padding"}`}>
      <Link to="/" className="h-full flex-1 md:grow-0 md:mr-[80px] mr-0 m-0 flex items-center text-lg leading-7 capitalize font-bold">
        <img src={require("../../assets/img/logo-light.svg").default} alt="" className="m-0 h-[60px]" />
        ChunJun
      </Link>
      <ul className="h-full md:flex items-center hidden">
        {links.map(l =>
          l.path ? (
            <Link to={l.path} key={l.path} className="header-link h-full flex items-center justify-center text-base relative w-[88px]">
              {l.name}
            </Link>
          ) : (
            <a target="_blank" className="header-link h-full flex items-center justify-center text-base relative w-[88px]" key={l.url} href={l.url}>
              {l.name}
            </a>
          )
        )}
      </ul>
      <div className="h-full flex items-center">
        <Burger opened={opened} id="burger" className="h-full flex md:hidden justify-center items-center" color={`${color ? "#333" : "#fff"}`} onClick={() => setOpened(!opened)} />
      </div>
      <AppDrawer
        opened={opened}
        handleClose={() => setOpened(false)}
        title={
          <div className="flex items-center text-2xl">
            <img src={require("../../assets/img/logo-light.svg").default} className="w-[50px]" alt="logo" />
            纯钧
          </div>
        }
        padding="xl"
        size="xl"
      >
        <div className="w-full flex flex-col">
          {links.map(link => {
            return link.path !== null ? (
              <Link key={link.name} to={link.path} className="text-xl py-0 px-[20px] mb-[20px]">
                {link.name}
              </Link>
            ) : (
              <a key={link.name} className="text-xl py-0 px-[20px] mb-[20px]" href={link.url} target="_blank">
                {link.name}
              </a>
            )
          })}
        </div>
      </AppDrawer>
    </header>
  )
}

export default AppHeader
