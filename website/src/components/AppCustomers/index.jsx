import { chunk } from 'lodash'
import './index.css'
import React, { useLayoutEffect, useState } from 'react'
import { CSSTransition, TransitionGroup } from 'react-transition-group'
const AppCustomers = () => {
  const imgs = [
    {
      img: require('../../assets/img/fudan.png').default,
      id: 'fuda'
    },
    {
      img: require('../../assets/img/API.png').default,
      id: 'API'
    },
    {
      img: require('../../assets/img/nat-4.png').default,
      id: 'nat-4'
    },
    {
      img: require('../../assets/img/nat-5.png').default,
      id: 'nat-5'
    },
    {
      img: require('../../assets/img/nat-6.png').default,
      id: 'nat-6'
    },
    {
      img: require('../../assets/img/nat-7.png').default,
      id: 'nat-7'
    },
    {
      img: require('../../assets/img/nat-8.png').default,
      id: 'nat-8'
    },
    {
      img: require('../../assets/img/nat-9.png').default,
      id: 'nat-9'
    },
    {
      img: require('../../assets/img/nat-10.png').default,
      id: 'nat-10'
    },
    {
      img: require('../../assets/img/pufa.png').default,
      id: 'pufa'
    },
    {
      img: require('../../assets/img/union.png').default,
      id: 'union'
    },
    {
      img: require('../../assets/img/zheda.png').default,
      id: 'zheda'
    },
    {
      img: require('../../assets/img/zhongxin.png').default,
      id: 'zhongxin'
    },
    {
      img: require('../../assets/img/nat-11.png').default,
      id: 'nat-11'
    },
    {
      img: require('../../assets/img/nat-12.png').default,
      id: 'nat-12'
    },
    {
      img: require('../../assets/img/nat-13.png').default,
      id: 'nat-13'
    },
    {
      img: require('../../assets/img/nat-14.png').default,
      id: 'nat-14'
    },
    {
      img: require('../../assets/img/nat-15.png').default,
      id: 'nat-15'
    },
    {
      img: require('../../assets/img/nat-16.png').default,
      id: 'nat-16'
    },
    {
      img: require('../../assets/img/nat-17.png').default,
      id: 'nat-17'
    },
    {
      img: require('../../assets/img/nat-18.png').default,
      id: 'nat-18'
    },
    {
      img: require('../../assets/img/nat-19.png').default,
      id: 'nat-19'
    },
    {
      img: require('../../assets/img/nat-20.png').default,
      id: 'nat-20'
    },
    {
      img: require('../../assets/img/nat-21.png').default,
      id: 'nat-21'
    },
    {
      img: require('../../assets/img/nat-22.png').default,
      id: 'nat-22'
    },
    {
      img: require('../../assets/img/nat-23.png').default,
      id: 'nat-23'
    },
    {
      img: require('../../assets/img/nat-24.png').default,
      id: 'nat-24'
    },
    {
      img: require('../../assets/img/nat-25.png').default,
      id: 'nat-25'
    },
    {
      img: require('../../assets/img/nat-26.png').default,
      id: 'nat-26'
    },
    {
      img: require('../../assets/img/nat-27.png').default,
      id: 'nat-27'
    },
    {
      img: require('../../assets/img/nat-28.png').default,
      id: 'nat-28'
    },
    {
      img: require('../../assets/img/nat-30.png').default,
      id: 'nat-30'
    }
  ]
  const imagesList = chunk(imgs, 5)
  const [activeList, setActive] = useState([...imagesList[0]])
  useLayoutEffect(() => {
    const timer = setInterval(() => {
      imagesList.push(imagesList.shift())
      setActive(origin => [])
      setTimeout(() => {
        setActive(origin => [...origin, ...imagesList[0]])
      }, 500)
    }, 1500)

    return () => {
      clearInterval(timer)
    }
  }, [])
  return (
    <section className="py-[50px] pb-[50px] w-[80%] m-auto">
      <p className="mb-[20px] text-[27px]">
        <span className="border-b-2 pb-[5px] text-[22px] font-bold  text-gray-700 border-gray-300 inline-block">
          合作伙伴
        </span>
      </p>
      <div className="flex w-full overflow-hidden justify-start">
        <TransitionGroup component={null}>
          {activeList.map(item => {
            return (
              <CSSTransition key={item.id} timeout={500} classNames="item">
                <div className="w-[20%] h-[90px] flex items-center justify-center">
                  <img className="max-w-[90%] max-h-full" src={item.img}></img>
                </div>
              </CSSTransition>
            )
          })}
        </TransitionGroup>
      </div>
    </section>
  )
}

export default AppCustomers
