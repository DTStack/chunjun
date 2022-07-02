import React, { useState } from "react"
import {navigate} from 'gatsby'

const IndexCom = ({children,className,to}) => {
    function go(params) {
        navigate(params)
    }
    return (
        <span onClick={()=>go(to)} className={'cursor-pointer '+className}>
           {children}
        </span>
    )
}
export default IndexCom