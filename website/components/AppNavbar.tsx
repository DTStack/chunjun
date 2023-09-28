import { Drawer, ActionIcon, Text } from '@mantine/core'
import Link from 'next/link'
import {
  Download,
  Signature,
  Code,
  Database,
  Tool,
  Heart
} from 'tabler-icons-react'
import Image from 'next/image'
import logo from '@/public/logo-dark.svg'

type Props = {
  opened: boolean
  changeOpened: () => void
}

const SEP = process.env.sep as string

// TODO:同步header的链接

const AppNavbar = (props: Props) => {
  const { opened, changeOpened } = props
  return (
    <Drawer
      overlayColor={'transparent'}
      padding="md"
      opened={opened}
      onClose={changeOpened}
    >
      <div className=" flex items-center mb-4">
        <Image
          priority
          src={logo}
          height={36}
          width={36}
          alt="logo of chunjun"
        ></Image>
        <Text className="text-xl font-bold capitalize flex items-center font-mono">
          Chunjun
        </Text>
      </div>
      <div className="h-[36px] flex items-center p-md cursor-pointer">
        <ActionIcon variant="light" color="violet" className="mx-2">
          <Signature />
        </ActionIcon>
        <Link href={'/documents/快速开始'}>
          <a className="font-mono">文档</a>
        </Link>
      </div>
      <div className="h-[36px] flex items-center p-md cursor-pointer">
        <ActionIcon variant="light" color="blue" className="mx-2">
          <Database />
        </ActionIcon>
        <Link href={`/examples/sql/binlog${SEP}binlog_stream`}>
          <a className="font-mono">SQL</a>
        </Link>
      </div>
      <div className="h-[36px] flex items-center p-md cursor-pointer">
        <ActionIcon color="orange" className="mx-2">
          <Code />
        </ActionIcon>
        <Link href={`/examples/json/binlog${SEP}binlog_hive`}>
          <a className="font-mono">JSON</a>
        </Link>
      </div>
      <div className="h-[36px] flex items-center p-md cursor-pointer">
        <ActionIcon color="red" className="mx-2">
          <Tool />
        </ActionIcon>
        <Link href={'/faq'}>
          <a className="font-mono">指南</a>
        </Link>
      </div>

      <div className="h-[36px] flex items-center p-md cursor-pointer">
        <ActionIcon color="green" className="mx-2">
          <Download />
        </ActionIcon>
        <a
          href="https://github.com/DTStack/chunjun/releases"
          target="blank"
          className="font-mono"
        >
          下载
        </a>
      </div>
      <div className="h-[36px] flex items-center p-md cursor-pointer">
        <ActionIcon color="red" className="mx-2">
          <Heart />
        </ActionIcon>
        <Link href={'/contributor'}>
          <a className="font-mono">贡献者</a>
        </Link>
      </div>
    </Drawer>
  )
}

export default AppNavbar
