#!/bin/bash
#参考钉钉文档 https://open-doc.dingtalk.com/microapp/serverapi2/qf2nxq
 sonarreport=$(curl -s http://172.16.100.198:8082/?projectname=dt-insight-engine/flinkx)
 curl -s "https://oapi.dingtalk.com/robot/send?access_token=e2718f7311243d2e58fa2695aa9c67a37760c7fce553311a32d53b3f092328ed" \
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