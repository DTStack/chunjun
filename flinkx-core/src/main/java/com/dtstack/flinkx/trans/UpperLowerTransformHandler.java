package com.dtstack.flinkx.trans;

import com.dtstack.flinkx.enums.OptEnum;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 大小写转换工具类
 *
 * @author liuwenjie
 * @date 2019/10/21 10:44
 */
public class UpperLowerTransformHandler implements Serializable {

    public static Logger LOG = LoggerFactory.getLogger(UpperLowerTransformHandler.class);

    public DataStream<Row> doUpperLowerTransform(DataStream<Row> dataStream, List optList) {
        if (null != optList && !optList.isEmpty()) {
            for (Object optObject : optList) {
                Map<String, Object> optMap = (Map<String, Object>) optObject;
                Integer position = Integer.valueOf((String) optMap.get("position"));
                String optName = (String) optMap.get("opt");
                dataStream = dataStream.map(new UpperLowerTransformMapFunction(position, optName));
            }
        }
        return dataStream;
    }

    class UpperLowerTransformMapFunction implements MapFunction<Row, Row> {

        /**
         * 字段索引位置
         */
        private Integer position;

        /**
         * 操作名字？大写？小写? 去重？过滤？
         */
        private String optName;

        public UpperLowerTransformMapFunction(Integer position, String optName) {
            this.position = position;
            this.optName = optName;
        }

        @Override
        public Row map(Row row) throws Exception {
            Object object = row.getField(position - 1);
            if (null != object) {
                String strValue = String.valueOf(object);
                if (OptEnum.upper.toString().equalsIgnoreCase(optName)) {
                    row.setField(position - 1, strValue.toUpperCase());
                    return row;
                }
                if (OptEnum.lower.toString().equalsIgnoreCase(optName)) {
                    row.setField(position - 1, strValue.toLowerCase());
                    return row;
                }
            }
            return row;
        }
    }
}
