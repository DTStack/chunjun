import doris from './image/doris-logo.png'
import elasticsearch from './image/elasticsearch-logo.png'
import emqx from './image/emqx-logo.png'
import ftp from './image/ftp-logo.png'
import hbase from './image/hbase-logo.png'
import hive from './image/hive-logo.jpeg'
import kafka from './image/kafka-logo.png'
import kudu from './image/kudu-logo.png'
import mongodb from './image/mongodb-logo.png'
import mysql from './image/mysql-logo.png'
import oracle from './image/oracle-logo.png'
import solr from './image/solr-logo.png'
import starrocks from './image/starrocks-logo.png'
import center from './image/chunjun.png'
const leftList = [emqx, ftp, hbase, hive, kafka, kudu, elasticsearch]
const leftMap = {
  0: 'emqx',
  1: 'ftp',
  2: 'hbase',
  3: 'hive',
  4: 'kafka',
  5: 'kudu',
  6: 'elasticsearch'
}
const rightList = [mongodb, mysql, oracle, solr, starrocks, doris]
const rightMap = {
  0: 'mongodb',
  1: 'mysql',
  2: 'oracle',
  3: 'solr',
  4: 'starrocks',
  5: 'doris'
}
export const getData = ({ width, height }) => {
  const nodes = getNodes({ width, height })
  const edges = getEdges(nodes)
  return {
    nodes,
    edges
  }
}

function getEdges (nodes) {
  const res = []
  const centerNode = nodes.find(node => node.center)
  nodes.forEach(node => {
    if (!node.center) {
      res.push({
        head: node.key,
        tail: centerNode.key
      })
    }
  })
  return res
}

function getNodes ({ width, height }) {
  const nodes = [
    {
      key: '22',
      text: 'binLog',
      poper: 'binLog',
      width: 50,
      height: 50,
      x: 100,
      y: 250
    },
    {
      key: 'g',
      img: rightList[0],
      poper: rightMap[5],
      width: 50,
      height: 50,
      x: 240,
      y: 400
    },
    {
      key: '222',
      text: 'logMiner',
      poper: 'logMiner',
      width: 50,
      height: 50,

      x: 64,
      y: 164
    },
    {
      key: 'b',
      img: leftList[1],
      poper: leftMap[1],
      width: 50,
      height: 50,
      x: 688,
      y: 98,
      size: 'big'
    },
    {
      key: 'c',
      img: leftList[2],
      poper: leftMap[2],
      width: 50,
      height: 50,
      x: 201,
      y: 32,
      size: 'big'
    },
    {
      key: 'd',
      img: leftList[3],
      poper: leftMap[3],
      width: 50,
      height: 50,
      x: 20,
      y: 340
    },
    {
      key: 'e',
      img: leftList[4],
      poper: leftMap[4],
      width: 50,
      height: 50,
      x: 52,
      y: 451,
      size: 'big'
    },
    {
      key: '333',
      img: leftList[5],
      poper: leftMap[5],
      width: 50,
      height: 50,
      x: 665,
      y: 500
    },
    {
      key: 'f',
      img: center,
      width: 100,
      height: 100,
      x: 377,
      y: 274,
      center: true
    },

    {
      key: 'h',
      img: rightList[1],
      poper: rightMap[1],
      width: 50,
      height: 50,
      x: 603,
      y: 52
    },
    {
      key: 'i',
      img: rightList[2],
      poper: rightMap[2],
      width: 50,
      height: 50,
      x: 191,
      y: 550
    },
    {
      key: 'j',
      img: rightList[3],
      poper: rightMap[3],
      width: 50,
      height: 50,
      x: 701,
      y: 245,
      size: 'big'
    },
    {
      key: '2',
      img: rightList[4],
      poper: rightMap[4],
      width: 50,
      height: 50,
      x: 600,
      y: 300,
      size: 'big'
    },
    {
      key: 'a',
      img: leftList[0],
      width: 50,
      height: 50,
      poper: leftMap[0],
      x: 539,
      y: 600
    }
  ].map(node => {
    if (node.size === 'big') {
      node.width = 200
      node.height = 100
    } else if (!node.center) {
      node.width = 150
      node.height = 75
    }
    return {
      ...node,
      x: (node.x / 835) * width,
      y: (node.y / 696) * height
    }
  })

  return nodes
}
