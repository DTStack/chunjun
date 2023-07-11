CREATE TABLE source
(
    id             int,
    name           varchar
) WITH (
      'connector' = 'restapi-x'
      ,'url' = 'http://dev.insight.dtstack.cn/api/streamapp/service/streamCatalogue/getCatalogue'
      ,'intervalTime'= '3000'
       ,'requestMode'='post'
        ,'decode'= 'json'
        ,'body'= '[
              {
              "key": "catalogueType",
              "value": "TaskDevelop",
              "type": "String"
              },
              {
                "key": "nodePid",
                "value": 75,
                "type": "int"
              },
              {
                "key": "isGetFile",
                "value": true,
                "type": "boolean"
              }
            ]'
        ,'header'='[
              {
                "key": "Cookie",
                "value": "experimentation_subject_id=ImZhODEyZGQ4LTllZDItNGUyZi1iZGVjLWZjOGMyNzQxNDk3NSI=--d51083bdcc50a5c9140cad574678f156d2bd6ff1; dt_language=zh; sysLoginType={\"sysId\":1,\"sysType\":0}; dt_user_id=1; dt_username=admin@dtstack.com; dt_can_redirect=false; dt_cookie_time=2021-06-20+17:59:08; dt_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0ZW5hbnRfaWQiOiIxIiwidXNlcl9pZCI6IjEiLCJ1c2VyX25hbWUiOiJhZG1pbkBkdHN0YWNrLmNvbSIsImV4cCI6MTYzOTEzMDIzNywiaWF0IjoxNjA4MDI2MjUyfQ.6cuG5Zuo0GzBtnfEPWDyFqAac1Umh3HbxycbEN5fGsg; dt_tenant_id=1; dt_tenant_name=DTStack租户; dt_is_tenant_admin=true; dt_is_tenant_creator=false; dt_product_code=RDOS; JSESSIONID=56C0AB7D249B1076A14CA305E871DAE8; DT_SESSION_ID=a761788a-e504-4d51-b1b0-128f9f8865e3"
              },
              {
                "key": "Content-Type",
                "value": "text/plain;charset=UTF-8"
              }
            ]'
            ,'column'='[
              {
                "name": "data.id",
                "type": "int"
              },
              {
                "name": "data.name",
                "type": "string"
              }
            ]'
      );

CREATE TABLE sink
(
    id             int,
    name      varchar
) WITH (
      'connector' = 'stream-x'
      );

insert into sink
select *
from source u;
