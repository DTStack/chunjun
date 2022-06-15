import React from "react"
import { useCallback, useEffect, useLayoutEffect, useState } from "react"
import "./index.css"
import { getData } from "./data"
import { Popover } from "antd"
import "antd/dist/antd.css"

function getPath({ head, tail }) {
  let startNode = JSON.parse(JSON.stringify(head))
  let endNode = JSON.parse(JSON.stringify(tail))
  startNode.x = startNode.x + startNode.width * 0.25
  startNode.y = startNode.y + startNode.height * 0.25
  endNode.x = endNode.x + endNode.width * 0.45
  endNode.y = endNode.y + endNode.height * 0.45
  let control1 = { x: 0, y: 0 }
  let control2 = { x: 0, y: 0 }
  const middle = (endNode.y - startNode.y) / 10
  control1.y = startNode.y + middle
  control1.x = startNode.x
  control2.y = control1.y
  control2.x = endNode.x
  return `M ${startNode.x} ${startNode.y} C ${control1.x} ${control1.y}, ${control2.x} ${control2.y}, ${endNode.x} ${endNode.y}`
}

const NodesRender = ({ nodes, setDraging, bool }) => {
  let centerNode = nodes.find(node => node.center)
  return (
    <>
      {centerNode ? (
        <div
          className={
            "absolute no-transition center-node bg-blue-200   rounded-full border cursor-move hover:shadow-lg"
          }
          style={{
            transform: "all 0",
            opacity: 0.8,
            top: centerNode.y - centerNode.height * 0.25 + "px",
            left: centerNode.x - centerNode.width * 0.25 + "px",
            height: centerNode.height * 1.5 + "px",
            width: centerNode.width * 1.5 + "px",
          }}
        ></div>
      ) : (
        ""
      )}
      {nodes.map(node => {
        return (
          <div
            key={node.key}
            onMouseDown={() => {
              setDraging(node.key)
            }}
            onMouseUp={() => {
              setDraging(null)
            }}
            className={
              node.center
                ? "absolute no-transition center-node rounded-full  cursor-move hover:shadow-lg"
                : "absolute no-transition flex justify-center items-center bg-white border border-gray-200 shadow-lg   cursor-move hover:shadow-sm p-3 rounded"
            }
            style={{
              top: node.y + "px",
              left: node.x + "px",
              transitionDelay: 0,
              transitionDuration: 0,
              height: node.height + "px",
              width: node.width + "px",
              backgroundImage: `url(${node.center ? node.img : ""})`,
              backgroundSize: "contain",
              backgroundPosition: "center",
            }}
          >
            {node.center ? (
              <Popover
                content={
                  <div className="select-none">
                    <p>CHUNJUN ～～</p>
                    <p>Chunjun是一个基于Flink的批流统一的数据同步工具</p>
                  </div>
                }
              >
                <div className="h-full w-full"></div>
              </Popover>
            ) : (
              <Popover
                content={
                  <div className="select-none p-3 text-3xl">
                    <p> {node.poper} </p>
                  </div>
                }
              >
                <div
                  className="h-full w-full font-bold text-2xl flex justify-center items-center"
                  style={{
                    backgroundImage: `url(${!node.center ? node.img : ""})`,
                    backgroundSize: "100% auto",
                    backgroundPosition: "center",
                    backgroundRepeat: "no-repeat",
                  }}
                >
                  {node.text}
                </div>
              </Popover>
            )}
          </div>
        )
      })}
    </>
  )
}

const EdgesRender = ({ edges, nodes, bool }) => {
  const [width, setWidth] = useState(0)
  const [height, setHeight] = useState(0)
  useLayoutEffect(() => {
    let dom = document.getElementById("painting")
    setWidth(dom.clientWidth)
    setHeight(dom.clientHeight)
  }, [])
  return (
    <>
      <div className="absolute bg-white w-full h-full t-0 l-0 ">
        <svg
          strokeWidth={0}
          stroke={"transparent"}
          className="border-0"
          height={height}
          width={width}
        >
          {edges.map((edge, i) => {
            let startNode = nodes.find(node => node.key === edge.head)
            let endNode = nodes.find(node => node.key === edge.tail)
            return (
              <g>
                <path
                  stroke="blue"
                  strokeOpacity="0.1"
                  strokeWidth="10"
                  fill="transparent"
                  d={getPath({ head: startNode, tail: endNode })}
                />
                <path
                  className="no-transition"
                  stroke="blue"
                  strokeOpacity="1"
                  strokeWidth="1"
                  fill="transparent"
                  d={getPath({ head: startNode, tail: endNode })}
                />
              </g>
            )
          })}
        </svg>
      </div>
    </>
  )
}

const Flow = () => {
  const [edges, setEdges] = useState([])
  const [nodeList, setList] = useState([])
  const [draging, setDraging] = useState(null)
  const [bool, setBool] = useState(false)
  useLayoutEffect(() => {
    let dom = document.getElementById("painting")
    let width = dom.clientWidth
    let height = dom.clientHeight
    const { nodes, edges } = getData({ width, height })
    setEdges(edges)
    setList(nodes)
    setBool(!bool)
  }, [])
  function moving({ nativeEvent }) {
    if (draging) {
      const target = nodeList.find(node => node.key === draging)
      target.x += nativeEvent.movementX
      target.y += nativeEvent.movementY
      setList(nodeList)
      setBool(!bool)
    }
  }
  useEffect(() => {}, [bool])
  return (
    <div
      id="painting"
      onMouseLeave={() => {
        setDraging(null)
      }}
      onMouseUp={() => {
        setDraging(null)
      }}
      onMouseMove={moving}
      className="w-full h-full  relative select-none"
    >
      <div className="w-full  relative h-full">
        <EdgesRender nodes={nodeList} bool={bool} edges={edges}></EdgesRender>
        <NodesRender
          setDraging={setDraging}
          bool={bool}
          nodes={nodeList}
        ></NodesRender>
      </div>
    </div>
  )
}

export default Flow
