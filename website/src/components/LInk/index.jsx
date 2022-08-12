import React from "react"
import { navigate } from "gatsby"
import { useContext } from "react"
import { ModeContext } from "../Context/index"
const IndexCom = ({ children, className, to, clickfn }) => {
  const m = useContext(ModeContext)
  function go(params) {
    navigate(params)
  }
  function onClick() {
    if (clickfn && m) {
      m.setmode(m.mode === "dark" ? "light" : "dark")
    } else {
      go(to)
    }
  }
  return (
    <span onClick={onClick} className={"cursor-pointer " + className}>
      {children}
    </span>
  )
}
export default IndexCom
