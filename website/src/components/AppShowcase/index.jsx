import React, { useEffect } from "react"
import Aos from "aos"

const AppShowcase = () => {
  const imgs = [
    {
      img: require("../../assets/img/fudan.png").default,
      id: "fuda",
    },
    {
      img: require("../../assets/img/API.png").default,
      id: "API",
    },
    {
      img: require("../../assets/img/nat-4.png").default,
      id: "nat-4",
    },
    {
      img: require("../../assets/img/nat-5.png").default,
      id: "nat-5",
    },
    {
      img: require("../../assets/img/nat-6.png").default,
      id: "nat-6",
    },
    {
      img: require("../../assets/img/nat-7.png").default,
      id: "nat-7",
    },
    {
      img: require("../../assets/img/nat-8.png").default,
      id: "nat-8",
    },
    {
      img: require("../../assets/img/nat-9.png").default,
      id: "nat-9",
    },
    {
      img: require("../../assets/img/nat-10.png").default,
      id: "nat-10",
    },
    {
      img: require("../../assets/img/pufa.png").default,
      id: "pufa",
    },
    {
      img: require("../../assets/img/union.png").default,
      id: "union",
    },
    {
      img: require("../../assets/img/zheda.png").default,
      id: "zheda",
    },
    {
      img: require("../../assets/img/zhongxin.png").default,
      id: "zhongxin",
    },
    {
      img: require("../../assets/img/nat-11.png").default,
      id: "nat-11",
    },
    {
      img: require("../../assets/img/nat-12.png").default,
      id: "nat-12",
    },
    {
      img: require("../../assets/img/nat-13.png").default,
      id: "nat-13",
    },
    {
      img: require("../../assets/img/nat-14.png").default,
      id: "nat-14",
    },
    {
      img: require("../../assets/img/nat-15.png").default,
      id: "nat-15",
    },
    {
      img: require("../../assets/img/nat-16.png").default,
      id: "nat-16",
    },
    {
      img: require("../../assets/img/nat-17.png").default,
      id: "nat-17",
    },
    {
      img: require("../../assets/img/nat-18.png").default,
      id: "nat-18",
    },
    {
      img: require("../../assets/img/nat-19.png").default,
      id: "nat-19",
    },
    {
      img: require("../../assets/img/nat-20.png").default,
      id: "nat-20",
    },
    {
      img: require("../../assets/img/nat-21.png").default,
      id: "nat-21",
    },
    {
      img: require("../../assets/img/nat-22.png").default,
      id: "nat-22",
    },
    {
      img: require("../../assets/img/nat-23.png").default,
      id: "nat-23",
    },
    {
      img: require("../../assets/img/nat-24.png").default,
      id: "nat-24",
    },
    {
      img: require("../../assets/img/nat-25.png").default,
      id: "nat-25",
    },
    {
      img: require("../../assets/img/nat-26.png").default,
      id: "nat-26",
    },
    {
      img: require("../../assets/img/nat-27.png").default,
      id: "nat-27",
    },
  ]
  useEffect(() => {
    Aos.init({
      duration: 1000,
    })
  }, [])
  return (
    <section data-aos="zoom-in" className="flex-padding flex flex-col items-center justify-center  bg-white">
      <h1 className="text-center w-full text-xl">成为我们的客户</h1>
      <div className="flex flex-wrap w-full bg-white p-3">
        {imgs.map(i => {
          return (
            <div key={i.id} className=" bg:w-1/6 md:w-1/6  sm:w-1/5 w-1/5  h-32 flex items-center p-2">
              <img src={i.img} alt="" className="w-full" />
            </div>
          )
        })}
      </div>
    </section>
  )
}

export default AppShowcase
