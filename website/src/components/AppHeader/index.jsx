import React, { useState } from "react"
import Link from "../Link"
import { Burger, Popover, Text } from "@mantine/core"
import AppDrawer from "../AppDrawer"
import { headerList } from "../../util"
import { useContext } from "react"
import { ModeContext } from "../Context"
const AppHeader = () => {
  const [opened, setOpened] = useState(false)
  const [visible, setVisible] = useState(false)
  const [show, setShow] = useState(false)
  const links = headerList
  const { mode } = useContext(ModeContext)
  return (
    <header className={`w-full dark:bg-[#1a1b1e] text-blue-400 h-[60px] border-bottom border-[#e9ecef]  bg-white sticky top-0 z-50 flex px-4 justify-between shadow-sm items-center}`}>
      <Link to="/" className="h-full hover:text-blue-400 font-bold flex-1 md:grow-0 md:mr-[80px] mr-0 m-0 flex items-center text-xl leading-7 capitalize">
        <img src={require("../../assets/img/logo-light.svg").default} alt="" className="m-0 h-[60px]" />
        ChunJun
      </Link>
      <ul className="h-full md:flex items-center hidden">
        {links.map(l =>
          l.path ? (
            !Array.isArray(l.path) ? (
              <Link to={l.path} clickfn={l.clickfn} key={l.name} className="header-link  text-blue-300 font-medium hover:text-blue-600 h-full flex items-center justify-center relative w-[60px]">
                {l.renderIcon ? <l.renderIcon mode={mode}></l.renderIcon> : l.icon ? l.icon : l.name}
              </Link>
            ) : (
              <Popover
                opened={visible}
                key={l.name}
                withArrow={false}
                onClose={() => setVisible(false)}
                target={
                  <a onClick={() => setVisible(v => !v)} className=" text-blue-300 header-link font-medium hover:text-blue-600 h-full flex items-center cursor-pointer justify-center relative w-[60px]">
                    {l.icon ? l.icon : l.name}
                  </a>
                }
                width={88}
                position="bottom"
              >
                <div className="flex space-y-1 flex-col">
                  {l.path.map(i => (
                    <Text size="md" key={i.name} className="text-left  uppercase font-normal font-mono hover:text-blue-600">
                      <Link to={i.link}>{i.name}</Link>
                    </Text>
                  ))}
                </div>
              </Popover>
            )
          ) : (
            <a target="_blank" key={l.name} clickfn={l.clickfn} rel="noreferrer" className="  text-blue-300 header-link  font-medium hover:text-blue-600  h-full flex items-center justify-center relative w-[60px]" href={l.url}>
              {l.icon ? l.icon : l.name}
            </a>
          )
        )}
      </ul>
      <div className="h-full flex items-center md:hidden">
        <Burger opened={opened} id="burger" className="h-full flex justify-center items-center" color="#333" onClick={() => setOpened(!opened)} />
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
        <div className="w-full flex flex-col space-y-3">
          {links.map(link => {
            return link.path !== null ? (
              !Array.isArray(link.path) ? (
                <Link key={link.name} to={link.path} className="text-xl py-0 px-[20px]">
                  {link.name}
                </Link>
              ) : (
                <Popover
                  opened={show}
                  key={link.name}
                  onClose={() => setShow(false)}
                  target={
                    <a onClick={() => setShow(v => !v)} className="text-xl py-0 px-[20px]">
                      {link.name}
                    </a>
                  }
                  width={150}
                  position="bottom"
                  placement="start"
                  gutter={10}
                  withArrow
                >
                  <div className="flex space-y-2 flex-col">
                    {link.path.map(i => (
                      <Text size="lg" key={i.name} className="text-center uppercase hover:text-blue-600">
                        <Link to={i.link}>{i.name}</Link>
                      </Text>
                    ))}
                  </div>
                </Popover>
              )
            ) : (
              <a key={link.name} rel="noreferrer" className="text-xl py-0 px-[20px]" href={link.url} target="_blank">
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
