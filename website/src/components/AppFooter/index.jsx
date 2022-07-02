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
          <div className="flex md:space-x-12 space-x-6">
            <div>
              <h3 className="mb-[20px]">文档</h3>
              <ul className="flex flex-col space-y-2">
                <Link className="footer-link" to="/documents">
                  快速上手
                </Link>
              </ul>
            </div>
            <div>
              <h3 className="mb-[20px]">社区</h3>
              <ul className="flex flex-col space-y-2">
                <a className="footer-link" href="https://github.com/DTStack/chunjun/issues">
                  Issue Tracker
                </a>
                <a className="footer-link" href="https://github.com/DTStack/chunjun/pulls">
                  Pull Requests
                </a>
                <a className="footer-link" href="https://github.com/DTStack/chunjun/discussions">
                  Discussions
                </a>
              </ul>
            </div>
            <div>
              <h3 className="mb-[20px]">更多</h3>
              <ul className="flex flex-col space-y-2">
                <Link className="footer-link" to="/">
                  博客
                </Link>
                <a className="footer-link" href="https://github.com/DTStack/chunjun">
                  Github
                </a>
              </ul>
            </div>
          </div>
        </div>
      </div>
      <div className="py-4 text-gray-600 text-base flex justify-center items-center">Apache LICENSE 2.0 Licensed, Copyright 2018-2022 Chunjun All Rights Reserved</div>
    </footer>
  )
}

export default AppFooter
