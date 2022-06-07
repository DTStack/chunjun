import "./index.scss"
import React from "react"
import { Card, Text, Image } from "@mantine/core"
import { Link } from "gatsby"

const AppFooter = () => {
  return (
    <footer className="footer flex-padding">
      <div className="footer__wrapper">
        <div className="footer__wrapper--left">
          <Card component="a" className="left__cards--card">
            <Card.Section>
              <Image
                src={require("../../assets/img/dt.jpg").default}
                height={181}
                width={177}
                alt="No way!"
              />
            </Card.Section>

            <Text weight={500} className="left__cards--title">
              联系我们
            </Text>

            <Text size="sm" className="left__cards--text">
              在这里你可以获取到最新的技术及产品知识
            </Text>
          </Card>
        </div>
        <div className="footer__wrapper--right">
          <div className="right__wrapper">
            <h3 className="right__wrapper--title">文档</h3>
            <ul className="right__wrapper--list">
              <Link className="right__wrapper--link" to="/">
                快速上手
              </Link>
            </ul>
          </div>
          <div className="right__wrapper">
            <h3 className="right__wrapper--title">社区</h3>
            <ul className="right__wrapper--list">
              <Link className="right__wrapper--link" to="/">
                Issue Tracker
              </Link>
              <Link className="right__wrapper--link" to="/">
                Pull Requests
              </Link>
              <Link className="right__wrapper--link" to="/">
                Discussions
              </Link>
            </ul>
          </div>
          <div className="right__wrapper">
            <h3 className="right__wrapper--title">更多</h3>
            <ul className="right__wrapper--list">
              <Link className="right__wrapper--link" to="/">
                博客
              </Link>
              <Link className="right__wrapper--link" to="/">
                Github
              </Link>
            </ul>
          </div>
        </div>
      </div>
      <div className="footer__copyright">
        Copyright © 2022 袋鼠云, Inc. Built with Gatsby.
      </div>
    </footer>
  )
}

export default AppFooter
