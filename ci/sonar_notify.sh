#!/bin/bash
#参考钉钉文档 https://open-doc.dingtalk.com/microapp/serverapi2/qf2nxq
 sonarreport=$(curl -s http://172.16.100.198:8082/?projectname=dt-insight-engine/flinkx)
 curl -s "https://oapi.dingtalk.com/robot/send?access_token=edb18a5400d20f62ffdb8eedae8318b95bd51021b980539edd4d5c504213eafa" \
   -H "Content-Type: application/json" \
   -d "{
     \"msgtype\": \"markdown\",
     \"markdown\": {
         \"title\":\"sonar代码质量\",
         \"text\": \"## sonar代码质量报告: \n
> [sonar地址](http://172.16.100.198:9000/dashboard?id=dt-insight-engine/flinkx) \n
> ${sonarreport} \n\"
     }
 }"