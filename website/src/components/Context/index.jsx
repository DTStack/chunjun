import React from "react"
import { createContext, useState } from "react"

export const ModeContext = createContext({
  mode: "dark",
  setmode: "",
})

const ProviderContext = ({ children }) => {
  const [mode, setmode] = useState("light")
  return (
    <ModeContext.Provider value={{ mode, setmode }}>
      <div className={mode}>{children}</div>
    </ModeContext.Provider>
  )
}

export default ProviderContext
