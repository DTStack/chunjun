import React from "react"
import { Drawer } from "@mantine/core"

const AppDrawer = ({
  children,
  title,
  opened,
  handleClose,
  position,
  padding,
  size,
  className,
}) => {
  return (
    <Drawer
      opened={opened}
      onClose={handleClose}
      title={title}
      padding={padding}
      size={size}
      className={className}
      position={position}
      overlayColor="transparent"
    >
      {children}
    </Drawer>
  )
}

export default AppDrawer
