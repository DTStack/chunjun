/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.http.util;

import org.apache.commons.lang3.tuple.Pair;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class XmlUtil {

    public static Map<String, Object> xmlParse(String xml, Map<String, Object> outmap) {
        Document document = null;
        try {
            document = DocumentHelper.parseText(xml);
        } catch (DocumentException e) {
            throw new RuntimeException(
                    "parse xml failed,the response values is not xml format and value is ->" + xml);
        }
        // 3.获取根节点
        Element rootElement = document.getRootElement();

        return elementTomap(rootElement, outmap);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> elementTomap(Element outele, Map<String, Object> outmap) {
        List<Element> list = outele.elements();
        int size = list.size();
        if (size == 0) {
            outmap.put(outele.getName(), outele.getTextTrim());
        } else {
            Map<String, Object> innermap = new HashMap<>();
            for (Element ele1 : list) {
                String eleName = ele1.getName();
                Object obj = innermap.get(eleName);
                if (obj == null) {
                    elementTomap(ele1, innermap);
                } else {
                    if (obj instanceof Map) {
                        List<Map<String, Object>> list1 = new ArrayList<Map<String, Object>>();
                        list1.add((Map<String, Object>) innermap.remove(eleName));
                        elementTomap(ele1, innermap);
                        list1.add((Map<String, Object>) innermap.remove(eleName));
                        innermap.put(eleName, list1);
                    } else if (obj instanceof String) {
                        List<String> list1 = new ArrayList<String>();
                        list1.add((String) innermap.remove(eleName));
                        elementTomap(ele1, innermap);
                        list1.add((String) innermap.remove(eleName));
                        innermap.put(eleName, list1);
                    } else {
                        elementTomap(ele1, innermap);
                        if (innermap.get(eleName) instanceof Map) {
                            Map<String, Object> listValue =
                                    (Map<String, Object>) innermap.get(eleName);
                            ((List<Map<String, Object>>) obj).add(listValue);
                            innermap.put(eleName, obj);
                        } else if (innermap.get(eleName) instanceof String) {
                            String listValue = (String) innermap.get(eleName);
                            ((List<String>) obj).add(listValue);
                            innermap.put(eleName, obj);
                        }
                    }
                }
            }
            outmap.put(outele.getName(), innermap);
        }
        return outmap;
    }

    public static LinkedHashMap<String, ArrayList<Pair<String, String>>> hbaseXmlParse(String xml) {
        Document document = null;
        try {
            document = DocumentHelper.parseText(xml);
        } catch (DocumentException e) {
            throw new RuntimeException(
                    "parse xml failed,the response values is not xml format and value is ->" + xml);
        }
        // 3.获取根节点
        Element rootElement = document.getRootElement();

        return hbaseElementTomap(rootElement);
    }

    // example
    // <CellSet>
    //    <Row key="MDIz">
    //        <Cell column="Y2YxOmFnZQ==" timestamp="1664247757125">MTM=</Cell>
    //        <Cell column="Y2YxOm5hbWU=" timestamp="1664247721773">a3lvdG9t</Cell>
    //    </Row>
    //    <Row key="MDI0">
    //        <Cell column="Y2YxOmFnZQ==" timestamp="1664247764018">MTY=</Cell>
    //    </Row>
    // </CellSet>

    public static LinkedHashMap<String, ArrayList<Pair<String, String>>> hbaseElementTomap(
            Element outele) {

        LinkedHashMap<String, ArrayList<Pair<String, String>>> data = new LinkedHashMap<>();
        List<Element> list = outele.elements();
        for (Element ele1 : list) {
            String eleName = ele1.getName();
            if (!eleName.equalsIgnoreCase("Row")) {
                continue;
            }
            Attribute key = ele1.attribute("key");
            if (key == null) {
                throw new RuntimeException(
                        "parse hbase xml data error, row element not has attribute key");
            }
            // cell
            ArrayList<Pair<String, String>> columns = new ArrayList<>();
            // add rowkey
            columns.add(Pair.of("rowkey", key.getValue()));
            for (Element element : ele1.elements()) {
                if (element.getName().equalsIgnoreCase("Cell")) {
                    Attribute column = element.attribute("column");
                    if (column == null) {
                        throw new RuntimeException(
                                "parse hbase xml data error, cell element has not attribute column");
                    }
                    columns.add(Pair.of(column.getValue(), element.getText()));
                }
            }

            data.put(key.getValue(), columns);
        }

        return data;
    }
}
