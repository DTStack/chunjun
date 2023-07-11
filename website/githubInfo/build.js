/**
 * @description 获取chunjun repo 成员的脚本
 *  使用node 执行后 成员信息会保存在同级目录的 member.json下
 */
const axios = require('axios')
const contributorsApi =
  'https://api.github.com/repos/DTStack/chunjun/contributors'
const fs = require('fs')
const path = require('path')
const fileName = 'members.json'
axios
  .get(contributorsApi)
  .then((res) => {
    fs.writeFileSync(
      path.resolve(__dirname, './' + fileName),
      JSON.stringify(res.data, null, '\t')
    )
  })
  .catch((error) => {
    console.error(error)
  })
