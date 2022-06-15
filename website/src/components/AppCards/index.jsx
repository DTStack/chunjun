import "./index.scss"
import { Card, Image, Text } from "@mantine/core"
import React, { useLayoutEffect } from "react"

const AppCards = () => {
  return (
    <section className="cards flex-padding">
      <Card component="a" className="cards__card">
        <Card.Section>
          <Image
            src={require("../../assets/img/easy-to-use@2x.png").default}
            height={136}
            width={136}
            alt="No way!"
          />
        </Card.Section>

        <Text weight={500} className="cards__card--title">
          easy to use
        </Text>

        <Text size="sm" className="cards__card--text">
          基于json、sql
          快速构建数据同步任务，你只需要关注数据源的结构信息即可，让您节省时间，专注于数据集成的开发。
        </Text>
      </Card>
      <Card component="a" className="cards__card">
        <Card.Section>
          <Image
            src={require("../../assets/img/flink-png@2x.png").default}
            width={136}
            height={136}
            alt="No way!"
          />
        </Card.Section>

        <Text weight={500} className="cards__card--title">
          基于 Flink
        </Text>

        <Text size="sm" className="cards__card--text">
          该项目构建于flink之上，基于flink 原生的input,output
          相关接口来实现多种数据源之间的数据传输，同时可以基于 flink
          自己扩展插件。
        </Text>
      </Card>
      <Card component="a" className="cards__card">
        <Card.Section>
          <Image
            src={require("../../assets/img/cloud-png@2x.png").default}
            width={136}
            height={136}
            alt="No way!"
          />
        </Card.Section>

        <Text weight={500} className="cards__card--title">
          关键特性
        </Text>

        <Text size="sm" className="cards__card--text">
          1. 多种数据源之间数据传输
          <br />
          2. 断点续传 3. 增量同步 4. 实时采集
          <br />
          5.脏数据管理 6. 实时数据还原
        </Text>
      </Card>
    </section>
  )
}

export default AppCards
