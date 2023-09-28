/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.dtstack.chunjun.connector.oracle.converter.OracleRawTypeConverter;

import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleRawTypeMapperTest {

    @Test
    public void testRegex() {
        String text = "tb(1,2)";
        String text1 = "tb(6)";
        String type2 = "tb(6)(2,3)";
        String type = "NUMBER";

        OracleRawTypeConverter converter = new OracleRawTypeConverter();

        String regex = "(?<name>[a-zA-Z]+)(?:\\(\\d+\\))?(?:\\((?<ps>\\d+(?:,\\d+)?)\\))?";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(type2);
        if (matcher.find()) {
            System.out.println(matcher.group("name"));
            System.out.println(matcher.group("ps"));
            System.out.println("======================");
            for (int i = 0; i <= matcher.groupCount(); i++) {
                System.out.println(matcher.group(i));
            }
        }

        /*String regex1 = "(?<name>[a-zA-Z]+)(?:\\(\\d+\\))?(?<ps>\\(\\d+(?:,\\d+)?\\))?";
        Pattern pattern = Pattern.compile(regex1);
        Matcher matcher = pattern.matcher(text3);
        if (matcher.find()){
            System.out.println(matcher.group("name"));
            System.out.println(matcher.group("ps"));
        }*/
        /*int p = 1;
        int s = 127;
        for(int i = 0 ;i<p;i++){
            System.out.print(0);
        }
        System.out.print(".");
        for(int i = 0 ;i<s;i++){
            System.out.print(5);
        }
        System.out.println('\n');*/
    }

    /*
    s > p,s-p 个 0 + p 个 数
     */
    @Test
    public void testRegex1() {
        int p = 1;
        int s = 127;
        System.out.print("0.");
        for (int i = 0; i < s - p; i++) {
            System.out.print(0);
        }
        for (int i = 0; i < p; i++) {
            System.out.print(5);
        }
        System.out.println('\n');
    }

    /*
    s < 0 ,p 个 数 + s 个 数
     */
    @Test
    public void testRegex2() {
        int p = 38;
        int s = -84;
        System.out.print("0.");
        for (int i = 0; i < p; i++) {
            System.out.print(4);
        }
        for (int i = 0; i < Math.abs(s); i++) {
            System.out.print(5);
        }
        System.out.println('\n');
    }

    @Test
    public void data() {
        double a =
                0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006d;
        //        float b =
        System.out.println(a);
    }
}
