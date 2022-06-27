import React from "react"
import { Card, Text, Image } from "@mantine/core"
import { Link } from "gatsby"

const AppFooter = () => {
  return (
    <footer className="text-center flex-padding">
      <div className="h-[558px] items-center flex flex-col md:flex-row justify-center">
        <div className="h-full w-full flex justify-center items-center bg-block bg-contain bg-center bg-no-repeat">
          <Card component="a" className="left__cards--card bg-transparent">
            <Card.Section className="flex justify-center items-center">
              <Image src={require("../../assets/img/dt.jpg").default} height={181} width={177} alt="No way!" />
            </Card.Section>

            <Text weight={500} className="left__cards--title">
              联系我们
            </Text>

            <Text size="sm" className="left__cards--text">
              在这里你可以获取到最新的技术及产品知识
            </Text>
          </Card>
        </div>
        <div className="h-full bg-block2 bg-no-repeat bg-center bg-contain w-full flex justify-center items-center">
          <div className="flex">
            <div className="mr-2">
              <h3 className="mb-[20px]">文档</h3>
              <ul className="flex flex-col">
                <Link className="footer-link" to="/">
                  快速上手
                </Link>
              </ul>
            </div>
            <div className="mr-2">
              <h3 className="mb-[20px]">社区</h3>
              <ul className="flex flex-col">
                <Link className="footer-link" to="/">
                  Issue Tracker
                </Link>
                <Link className="footer-link" to="/">
                  Pull Requests
                </Link>
                <Link className="footer-link" to="/">
                  Discussions
                </Link>
              </ul>
            </div>
            <div>
              <h3 className="mb-[20px]">更多</h3>
              <ul className="flex flex-col">
                <Link className="footer-link" to="/">
                  博客
                </Link>
                <Link className="footer-link" to="/">
                  Github
                </Link>
              </ul>
            </div>
          </div>
        </div>
      </div>
      <div className="h-[20px] text-gray-600 text-sm flex justify-center items-center">Copyright © 2022 袋鼠云, Inc. Built with Gatsby.</div>
    </footer>
  )
}

export default AppFooter
