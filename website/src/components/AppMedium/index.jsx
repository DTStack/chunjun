import React, { useEffect } from 'react'
import Aos from 'aos'
import { Card, Text, Button, Group } from '@mantine/core'
import { navigate } from 'gatsby-link'

const AppMedium = () => {
  useEffect(() => {
    Aos.init({
      duration: 1000
    })
  }, [])
  // data-aos="fade-up"
  return <section className="w-full dark:bg-[#1a1b1e] lg:p-6 p-4 md:h-[500px] h-auto">
    <h1 data-aos="fade-up" className="font-mono mb-24 md:text-3xl text-xl capitalize text-center font-bold from-green-400 bg-gradient-to-r  to-purple-600 bg-clip-text text-transparent">easy to learn. easy to use.</h1>
    <div className="grid md:grid-cols-3 grid-cols-1 md:gap-6 gap-3">
      <Card data-aos="fade-up" shadow="none" p="md" data-aos-delay="0">
        <Card.Section className='flex justify-center'>
          <img src={require('../../assets/svg/apacheflink.svg').default} alt="flink" height={100} className='object-contain' />
        </Card.Section>

        <Group position="apart" className='my-3 text-center'>
          <Text weight={500} className="font-bold w-full text-lg capitalize">基于Flink</Text>
        </Group>

        <Text size="md" className='text-gray-600 leading-6 text-center capitalize'>
          兼容Flink SQL语法,如果您使用过Flink,那么您可以很快的上手chunjun!
        </Text>

        <Button onClick={() => navigate('/documents')} variant="light" color="green" fullWidth className='mt-[14px]'>
          阅读文档
        </Button>
      </Card>
      <Card data-aos="fade-up" shadow="none" p="md" data-aos-delay="150">
        <Card.Section className='flex justify-center'>
          <img src={require('../../assets/svg/docker.svg').default} alt="flink" height={100} className='object-contain' />
        </Card.Section>

        <Group position="apart" className='my-3 text-center'>
          <Text weight={500} className="font-bold w-full text-lg capitalize">部署支持</Text>
        </Group>

        <Text size="md" className='text-gray-600 leading-6 text-center capitalize'>
          支持Docker一键部署、K8S 部署运行,在您所青睐的平台上部署chunjun应用。
        </Text>

        <Button onClick={() => navigate('/documents')} variant="light" color="blue" fullWidth className='mt-[14px]'>
          如何部署
        </Button>
      </Card>
      <Card data-aos="fade-up" shadow="none" p="md" data-aos-delay="300">
        <Card.Section className='flex justify-center'>
          <img src={require('../../assets/svg/flex.svg').default} alt="flink" height={100} className='object-contain' />
        </Card.Section>

        <Group position="apart" className='my-3 text-center'>
          <Text weight={500} className="font-bold w-full text-lg capitalize">易拓展，高灵活</Text>
        </Group>

        <Text size="md" className='text-gray-600 leading-6 text-center capitalize'>
          插件开发者无需关心其他插件的代码逻辑，新拓展的数据源插件可以与现有数据源插件即时互通。
        </Text>

        <Button onClick={() => navigate('/documents')} variant="light" color="violet" fullWidth className='mt-[14px]'>
          拓展插件
        </Button>
      </Card>
    </div>
  </section>
}

export default AppMedium
