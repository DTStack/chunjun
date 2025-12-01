package com.dtstack.chunjun.connector.http.client;

import com.dtstack.chunjun.connector.http.common.HttpRestConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.util.GsonUtil;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.jayway.jsonpath.JsonPath;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.Map;

/** @Description 离线任务 @Author lianggao @Date 2023/6/1 下午5:50 */
public class OfflineJsonResponseParse extends ResponseParse {

    private String responseValue;
    private HttpRequestParam requestParam;
    private final Gson gson;
    private Iterator<Object> iterator;
    /** true:single true:array false:single false:array */
    String parserFlag;

    String jsonPath;

    public OfflineJsonResponseParse(HttpRestConfig config, AbstractRowConverter converter) {
        super(config, converter);
        this.gson = GsonUtil.GSON;
        this.jsonPath = config.getJsonPath();
        this.parserFlag = !StringUtils.isBlank(jsonPath) + ":" + config.getReturnedDataType();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public ResponseValue next() throws Exception {
        String rowJson = gson.toJson(iterator.next());
        Map<String, Object> data =
                DefaultRestHandler.gson.fromJson(rowJson, GsonUtil.gsonMapTypeToken);
        return new ResponseValue(converter.toInternal(data), requestParam, responseValue);
    }

    @Override
    public void parse(String responseValue, int responseStatus, HttpRequestParam requestParam) {
        this.responseValue = responseValue;
        this.requestParam = requestParam;
        runParseJson();
    }

    public void runParseJson() {
        switch (parserFlag) {
            case "false:array":
                iterator = JsonPath.read(responseValue, "$");
                break;
            case "true:single":
                Object read = JsonPath.read(responseValue, jsonPath);
                iterator = Lists.newArrayList(read).iterator();
                break;
            case "true:array":
                Object eval = JsonPath.read(responseValue, jsonPath);
                if (eval instanceof net.minidev.json.JSONArray) {
                    iterator = ((net.minidev.json.JSONArray) eval).iterator();
                } else {
                    // 如果为null 则直接报错，返回解析错误的数据
                    if (eval == null) {
                        throw new RuntimeException(
                                "response parsing is incorrect Please check the conf ,get response is"
                                        + responseValue);
                    }
                    iterator = Lists.newArrayList(eval).iterator();
                }
                break;
            default:
                Lists.newArrayList(responseValue);
        }
    }
}
