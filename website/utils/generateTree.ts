import FileTree from '@/types/FileTree'

export const generateTree = (allPaths: string[]) => {
  const map = new Map<string, boolean>()

  const tree: FileTree[] = []

  const generate = (path: string, obj: FileTree[]): FileTree[] => {
    if (!path.includes('/')) {
      obj.push({
        label: path.split('.')[0],
        path: path.split('.')[0],
        category: 'file'
      })
    } else {
      const t = path.split('/')[0]
      const f = obj.find((o) => o.label === t)
      const current = path.split('/').slice(1).join('/')
      if (!f) {
        obj.push({
          label: path.split('/')[0],
          category: 'dir',
          children: []
        })
        return generate(
          current,
          obj.find((o) => o.label === t)?.children as FileTree[]
        )
      } else {
        return generate(current, f?.children as FileTree[])
      }
    }
    return obj
  }

  for (const link of allPaths) {
    if (!link.includes('/')) {
      tree.push({
        label: link.split('.')[0],
        category: 'file'
      })
      continue
    }
    map.set(link.split('/')[0], true)
  }

  for (const m of map) {
    const root: FileTree = {
      label: m[0],
      children: [],
      category: 'dir'
    }
    for (const link of allPaths) {
      if (link.split('/')[0] === root.label) {
        generate(
          link.split('/').slice(1).join('/'),
          root.children as FileTree[]
        )
      }
    }
    tree.push(root)
  }
  return tree
}
