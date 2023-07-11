import type { GetStaticPropsContext, NextPage } from 'next';
import { Button, useMantineColorScheme, Text, Card, Modal, useMantineTheme } from '@mantine/core';
import { Prism } from '@mantine/prism';
import Link from 'next/link';
import Image from 'next/image';
import { useEffect, useState } from 'react';
import Aos from 'aos';
import { BrandGithub, Affiliate } from 'tabler-icons-react';
import { companies } from '@/config/companies';
import AppFooter from '@/components/AppFooter';
import { useTranslation } from '@/next-export-i18n';

const SEP = process.env.sep;

const Home: NextPage = () => {
  const { colorScheme } = useMantineColorScheme();
  const [opened, setOpened] = useState<boolean>(false);
  const theme = useMantineTheme();
  const { t } = useTranslation();

  useEffect(() => {
    Aos.init({
      duration: 1000
    });
  }, [colorScheme]);

  return (
    <>
      <Modal
        opened={opened}
        onClose={() => setOpened(false)}
        title='请扫描二维码进群'
        overlayColor={theme.colorScheme === 'dark' ? theme.colors.dark[3] : theme.colors.gray[1]}
        centered
      >
        <div className='flex justify-center items-center'>
          <Image
            priority
            src='/assets/img/dingding.jpg'
            alt='钉钉群二维码'
            height={500}
            width={400}
            className='mx-auto'
          />
        </div>
      </Modal>
      <div className='2xl:max-w-[60vw] mx-auto'>
        <section
          className={`relative flex flex-col justify-center items-center section-padding ${
            colorScheme === 'light' ? 'bg-data-light' : 'bg-data-dark'
          } bg-center bg-no-repeat md:bg-contain bg-cover`}
        >
          <Text
            data-aos='fade-up'
            data-aos-delay='300'
            className='capitalize md:text-5xl md:mb-12 mb-2 text-3xl font-nunito'
          >
            <span
              className={`${
                colorScheme === 'light'
                  ? null
                  : 'bg-gradient-to-r from-blue-400 via-indigo-500 to-purple-600 bg-clip-text text-transparent'
              }`}
            >
              chunjun&nbsp;
            </span>
            纯钧
          </Text>
          <Text
            data-aos='fade-up'
            data-aos-delay='350'
            component='p'
            className='capitalize md:text-xl md:w-1/2 w-ful leading-relaxed text-base md:mb-12 mb-8 font-roboto'
          >
            是一款稳定、易用、高效、批流一体的数据集成框架，目前基于实时计算引擎Flink实现多种异构数据源之间的数据同步与计算，已在上千家公司部署且稳定运行。
          </Text>
          <div className='flex items-center mb-8'>
            <Button
              leftIcon
              size='lg'
              component='a'
              data-aos='fade-right'
              data-aos-delay='400'
              rightIcon={<BrandGithub />}
              href='https://github.com/DTStack/chunjun'
              variant='light'
              className={`text-xl capitalize font-nunito ${
                colorScheme === 'light' ? 'bg-blue-50' : null
              } mr-2`}
            >
              github
            </Button>
            <Link href={`/documents/快速开始`} passHref>
              <Button
                component='a'
                size='lg'
                variant='light'
                color='violet'
                data-aos='fade-left'
                data-aos-delay='400'
                rightIcon={<Affiliate />}
                className={`text-xl capitalize font-nunito ${
                  colorScheme === 'light' ? 'bg-indigo-50' : null
                } ml-2`}
              >
                {t('Home.quickStart')}
              </Button>
            </Link>
          </div>
          <div className='md:w-1/2 w-full font-roboto'>
            <Prism language='bash' className='font-nunito'>
              git clone https://github.com/DTStack/chunjun.git
            </Prism>
          </div>
        </section>
        <section className='flex flex-col section-padding'>
          <div className='flex flex-col items-center justify-center md:mb-16 mb-12'>
            <Text className='md:text-2xl text-xl text-center'>核心功能</Text>
          </div>
          <div className='grid md:grid-cols-3 md:grid-rows-2 grid-cols-1 md:gap-6 gap-4'>
            <Card
              shadow='none'
              radius='md'
              p='lg'
              data-aos='zoom-in'
              data-aos-delay='100'
              withBorder={true}
              className='hover:scale-105 transition-all duration-150'
            >
              <Card.Section className='text-center'>
                <Image
                  priority
                  src='/assets/svg/code.svg'
                  height={70}
                  width={70}
                  alt='Banner'
                />
              </Card.Section>

              <Text size='sm' color='dimmed' className='text-center'>
                基于json、sql 构建任务
              </Text>
            </Card>
            <Card
              shadow='none'
              radius='md'
              data-aos='zoom-in'
              data-aos-delay='120'
              withBorder={true}
              className='hover:scale-105 transition-all duration-150'
            >
              <Card.Section className='text-center'>
                <Image
                  src='/assets/svg/trans.svg'
                  priority
                  height={70}
                  width={70}
                  alt='Banner'
                />
              </Card.Section>

              <Text size='sm' color='dimmed' className='text-center'>
                支持多种异构数据源之间数据传输
              </Text>
            </Card>
            <Card
              shadow='none'
              radius='md'
              data-aos='zoom-in'
              data-aos-delay='140'
              withBorder={true}
              className='hover:scale-105 transition-all duration-150'
            >
              <Card.Section className='text-center'>
                <Image
                  src='/assets/svg/sync.svg'
                  height={70}
                  priority
                  width={70}
                  alt='Banner'
                />
              </Card.Section>

              <Text size='sm' color='dimmed' className='text-center'>
                支持断点续传、增量同步
              </Text>
            </Card>
            <Card
              shadow='none'
              radius='md'
              data-aos='zoom-in'
              data-aos-delay='160'
              withBorder={true}
              className='hover:scale-105 transition-all duration-150'
            >
              <Card.Section className='text-center'>
                <Image
                  src='/assets/svg/hcs_sgw.svg'
                  height={70}
                  width={70}
                  priority
                  alt='Banner'
                />
              </Card.Section>

              <Text size='sm' color='dimmed' className='text-center'>
                支持任务脏数据存储管理
              </Text>
            </Card>
            <Card
              shadow='none'
              radius='md'
              data-aos='zoom-in'
              data-aos-delay='180'
              withBorder={true}
              className='hover:scale-105 transition-all duration-150'
            >
              <Card.Section className='text-center'>
                <Image
                  src='/assets/svg/datav.svg'
                  height={70}
                  width={70}
                  priority
                  alt='Banner'
                />
              </Card.Section>

              <Text size='sm' color='dimmed' className='text-center'>
                支持Schema同步
              </Text>
            </Card>
            <Card
              shadow='none'
              radius='md'
              data-aos='zoom-in'
              data-aos-delay='200'
              withBorder={true}
              className='hover:scale-105 transition-all duration-150'
            >
              <Card.Section className='text-center'>
                <Image
                  src='/assets/svg/collect.svg'
                  height={70}
                  width={70}
                  priority
                  alt='Banner'
                />
              </Card.Section>

              <Text size='sm' color='dimmed' className='text-center'>
                支持RDBS数据源实时采集
              </Text>
            </Card>
          </div>
        </section>
      </div>
      <div className={`${colorScheme === 'light' ? 'bg-gray-50' : null}`}>
        <section className='flex md:flex-row flex-col-reverse md:h-96 h-auto py-8 2xl:max-w-[60vw] mx-auto'>
          <div
            data-aos='fade-up'
            data-aos-delay='100'
            className='relative md:w-1/2 w-full h-full md:space-y-0 space-y-3 md:p-0 p-3'
          >
            <Card
              shadow='lg'
              p='lg'
              radius='md'
              withBorder
              className='md:absolute relative md:w-64 md:left-[12%] md:top-[10%] z-0 hover:z-20 hover:scale-110 transition-all duration-200 ease-in-out'
            >
              <Card.Section className='text-center p-4'>
                <Image
                  src='/assets/img/flink-png@2x.png'
                  height={80}
                  width={80}
                  priority
                  alt='Banner'
                />
              </Card.Section>
              <Text className='text-xl font-bold capitalize text-center'>基于flink</Text>
              <Text className='text-base'>
                兼容Flink SQL语法,如果您使用过Flink,那么您可以很快的上手Chunjun!
              </Text>

              <Link href='/documents/快速开始' passHref>
                <Button
                  variant='light'
                  color='green'
                  fullWidth
                  mt='md'
                  radius='md'
                  className='capitalize'
                  component='a'
                >
                  阅读我们的文档
                </Button>
              </Link>
            </Card>
            <Card
              shadow='lg'
              p='lg'
              radius='md'
              withBorder
              className='md:absolute md:w-64 relative md:left-[40%] z-10 hover:z-20 hover:scale-110 transition-all duration-200 ease-in-out'
            >
              <Card.Section className='text-center p-4'>
                <Image
                  src='/assets/svg/docker.svg'
                  height={80}
                  priority
                  width={80}
                  alt='Banner'
                />
              </Card.Section>
              <Text className='text-xl font-bold capitalize text-center'>快速部署</Text>
              <Text className='text-base'>
                支持Docker一键部署、K8S 部署运行,在您所青睐的平台上部署Chunjun应用。
              </Text>

              <Link href='/documents/ChunJun通用配置详解' passHref>
                <Button
                  variant='light'
                  color='blue'
                  fullWidth
                  mt='md'
                  radius='md'
                  className='capitalize'
                  component='a'
                >
                  开始了解如何部署
                </Button>
              </Link>
            </Card>
            <Card
              shadow='lg'
              p='lg'
              radius='md'
              withBorder
              className='md:absolute relative md:w-64 md:left-[70%] 2xl:left-[60%] md:top-[15%] z-0 hover:z-20 hover:scale-110 transition-all duration-200 ease-in-out'
            >
              <Card.Section className='text-center p-4'>
                <Image
                  src='/assets/svg/flex.svg'
                  height={80}
                  width={80}
                  priority
                  alt='Banner'
                />
              </Card.Section>
              <Text className='text-xl font-bold capitalize text-center'>易拓展，高灵活</Text>
              <Text className='text-base'>
                插件开发者无需关心其他插件的代码逻辑，新拓展的数据源插件可以与现有数据源插件即时互通。
              </Text>

              <Link href={`/documents/开发者指南${SEP}如何自定义插件`} passHref>
                <Button
                  variant='light'
                  color='violet'
                  fullWidth
                  mt='md'
                  radius='md'
                  className='capitalize'
                  component='a'
                >
                  了解更多
                </Button>
              </Link>
            </Card>
          </div>
          <div className='md:w-1/2 w-full flex flex-col items-center justify-center px-4 md:mb-0 mb-4'>
            <Text className='text-center text-2xl font-bold md:mb-8 mb-4'>上手灵活，使用简单</Text>
            <Text
              data-aos='fade-up'
              data-aos-delay='400'
              className='text-base subpixel-antialiased tracking-tight font-nunito capitalize md:w-2/3 w-full'
            >
              &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;您无需担心Chunjun的上手难度,也无需担忧她在生产环境的表现。纯钧(ChunJun)已将不同的数据库抽象成了
              <span className='underline decoration-yellow-500 decoration-wavy decoration-2'>
                reader/source
              </span>
              插件,
              <span className='underline decoration-green-500 decoration-wavy decoration-2'>
                writer/sink
              </span>
              插件 和
              <span className='underline decoration-indigo-500 decoration-wavy decoration-2'>
                lookup维表
              </span>
              插件。
            </Text>
          </div>
        </section>
      </div>
      <div className='2xl:max-w-[60vw] mx-auto'>
        <section className='md:px-24 md:py-16 p-8 flex items-center md:flex-row flex-col'>
          <div className='flex md:flex-col space-y-3 md:w-1/4 w-full '>
            <Text className='text-base font-roboto font-bold'>目前已在</Text>
            <Text
              data-aos='fade-up'
              data-aos-delay='300'
              color={colorScheme === 'light' ? 'violet' : 'blue'}
              className='md:text-6xl text-3xl font-roboto font-bold m-0'
            >
              1,000
            </Text>
            <Text className='font-bold'>余家公司和机构稳定运行</Text>
          </div>
          <div className='grid md:grid-cols-6 grid-cols-3 gap-4 flex-1 justify-items-center'>
            {companies.map((com, index) => (
              <Image
                data-aos='fade-up'
                data-aos-delay={120 * index}
                className='shadow-lg rounded-sm'
                src={'' + com}
                height={48}
                width={100}
                priority
                alt='company'
                key={com}
              />
            ))}
          </div>
        </section>
      </div>
      <section className={`section-padding ${colorScheme === 'light' ? 'bg-gray-50' : null}`}>
        <Text className='text-center text-3xl md:mb-12 mb-8'>想要加入我们?</Text>
        <div className='flex md:flex-row flex-col justify-center'>
          <Card
            shadow='none'
            radius='md'
            className='md:mr-24 md:w-64 w-full md:mb-0 mb-6'
            data-aos='fade-up'
          >
            <Card.Section className='text-center p-4'>
              <Image
                height={120}
                width={120}
                priority
                src={`/assets/img/${colorScheme === 'light' ? 'github' : 'github-dark'}.svg`}
                alt='github'
              />
            </Card.Section>

            <Text size='sm' color='dimmed' className='capitalize text-center'>
              在github上寻找我们的组织
            </Text>

            <Button
              variant='light'
              color='blue'
              fullWidth
              mt='md'
              component='a'
              href='https://github.com/DTStack/chunjun/graphs/contributors'
              radius='md'
              className='capitalize font-nunito'
            >
              ok, i got it
            </Button>
          </Card>
          <Card data-aos='fade-up' shadow='none' radius='md' className='md:ml-24 md:w-64 w-full'>
            <Card.Section className='text-center p-4'>
              <Image
                height={120}
                width={120}
                src='/assets/svg/dingding.svg'
                priority
                alt='github'
              />
            </Card.Section>

            <Text size='sm' color='dimmed' className='capitalize text-center'>
              加入我们的钉钉群
            </Text>

            <Button
              variant='light'
              color='indigo'
              fullWidth
              mt='md'
              radius='md'
              className='capitalize'
              onClick={() => setOpened(true)}
            >
              现在加入
            </Button>
          </Card>
        </div>
      </section>
      <div>
        <AppFooter />
      </div>
    </>
  );
};

export default Home;
