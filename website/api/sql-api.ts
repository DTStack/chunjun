import fs from 'fs'
import { join } from 'path'

const SEP = process.env.sep as string
const SQL_PATH = process.env.sql as string

const sqlDirectory = join(process.cwd(), SQL_PATH)

export const getAllSqlPaths = (root = SQL_PATH, allFiles: string[] = []) => {
  const currentDir = join(process.cwd(), root)

  const files = fs.readdirSync(currentDir)

  for (const file of files) {
    if (file.includes('.')) {
      allFiles.push(
        root === SQL_PATH
          ? file
          : `${root.split('/').slice(3).join('/')}/${file}`
      )
    } else {
      getAllSqlPaths(`${root}/${file}`, allFiles)
    }
  }

  return allFiles.filter((file) => file.includes('.sql'))
}

export const getSqlByName = (name: string) => {
  const realSlug = name
    .split(SEP)
    .join('/')
    .replace(/\.sql$/, '')
  const fullPath = join(sqlDirectory, `${realSlug}.sql`)
  const fileContents = fs.readFileSync(fullPath, 'utf8')
  return {
    slug: realSlug,
    content: fileContents
  }
}

export const getAllSqlFiles = () => {
  const fileNames = getAllSqlPaths()
  const sqlFiles = fileNames.map((name) => getSqlByName(name))
  return sqlFiles
}
