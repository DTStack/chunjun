import fs from 'fs'
import { join } from 'path'

const SEP = process.env.sep as string
const JSON_PATH = process.env.json as string

const jsonDirectory = join(process.cwd(), JSON_PATH)

export const getAllJsonPaths = (root = JSON_PATH, allFiles: string[] = []) => {
  const currentDir = join(process.cwd(), root)

  const files = fs.readdirSync(currentDir)

  for (const file of files) {
    if (file.includes('.')) {
      allFiles.push(
        root === JSON_PATH
          ? file
          : `${root.split('/').slice(3).join('/')}/${file}`
      )
    } else {
      getAllJsonPaths(`${root}/${file}`, allFiles)
    }
  }

  return allFiles.filter((file) => file.includes('.json'))
}

export const getJsonByName = (name: string) => {
  const realSlug = name
    .split(SEP)
    .join('/')
    .replace(/\.json$/, '')
  const fullPath = join(jsonDirectory, `${realSlug}.json`)
  const fileContents = fs.readFileSync(fullPath, 'utf8')
  return {
    slug: realSlug,
    content: fileContents
  }
}

export const getAllJsonFiles = () => {
  const fileNames = getAllJsonPaths()
  const jsonFiles = fileNames.map((name) => getJsonByName(name))
  return jsonFiles
}
