import React, { useState } from "react"
import { Link } from "gatsby"
import { Burger, ActionIcon } from "@mantine/core"
import { Translate } from "@icon-park/react"
import AppDrawer from "../AppDrawer"
import "./index.scss"
import { headerList } from "../../util"

const AppHeaderWhite = () => {
  // const options = [
  //   {
  //     label: "简体中文",
  //     key: "zh_Hans",
  //   },
  //   {
  //     label: "English",
  //     key: "en",
  //   },
  // ]
  const [opened, setOpened] = useState(false)
  // const title = opened ? "Close navigation" : "Open navigation"

  const links = headerList

  return (
    <div className="white">
      <header className="header  flex-padding">
        <Link to="/" className="header__logo">
          <img
            src={require("../../assets/img/logo-light.svg").default}
            alt=""
          />
          ChunJun
        </Link>
        <ul className="header__links">
          {links.map(l => (
            <Link to={l.path} key={l.path} className="header__links--link ">
              {l.name}
            </Link>
          ))}
        </ul>
        <div className="header__utilities">
          <ActionIcon variant="transparent" id="i18n">
            <Translate theme="outline" size="24" fill="#fff" />
          </ActionIcon>
          <Burger
            opened={opened}
            id="burger"
            color="#fff"
            onClick={() => setOpened(!opened)}
          />
        </div>
        <AppDrawer
          opened={opened}
          handleClose={() => setOpened(false)}
          title={
            <div className="drawer__header">
              <img
                src={require("../../assets/img/logo-light.svg").default}
                width={50}
                alt="logo"
                style={{ margin: 0 }}
              />
              纯钧
            </div>
          }
          padding="xl"
          size="xl"
          className="header__drawer"
        >
          <div className="drawer__wrapper">
            {links.map(link => (
              <Link
                key={link.name}
                to={link.path}
                className="drawer__wrapper--link"
              >
                {link.name}
              </Link>
            ))}
          </div>
        </AppDrawer>
      </header>
    </div>
  )
}

export default AppHeaderWhite
