/**
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

package com.dtstack.flinkx.util;

import java.math.BigInteger;

/**
 * Range Split Utilities
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public final class RangeSplitUtil {

    public static long[] doLongSplit(long left, long right, int expectSliceNumber) {
        BigInteger[] result = doBigIntegerSplit(BigInteger.valueOf(left),
                BigInteger.valueOf(right), expectSliceNumber);
        long[] returnResult = new long[result.length];
        for (int i = 0, len = result.length; i < len; i++) {
            returnResult[i] = result[i].longValue();
        }
        return returnResult;
    }

    public static BigInteger[] doBigIntegerSplit(BigInteger left, BigInteger right, int expectSliceNumber) {
        if (expectSliceNumber < 1) {
            throw new IllegalArgumentException(String.format(
                    "切分份数不能小于1. 此处:expectSliceNumber=[%s].", expectSliceNumber));
        }

        if (null == left || null == right) {
            throw new IllegalArgumentException(String.format(
                    "对 BigInteger 进行切分时，其左右区间不能为 null. 此处:left=[%s],right=[%s].", left, right));
        }

        if (left.compareTo(right) == 0) {
            return new BigInteger[]{left, right};
        } else {
            // 调整大小顺序，确保 left < right
            if (left.compareTo(right) > 0) {
                BigInteger temp = left;
                left = right;
                right = temp;
            }

            //left < right
            BigInteger endAndStartGap = right.subtract(left);

            BigInteger step = endAndStartGap.divide(BigInteger.valueOf(expectSliceNumber));
            BigInteger remainder = endAndStartGap.remainder(BigInteger.valueOf(expectSliceNumber));

            //remainder 不可能超过expectSliceNumber,所以不需要检查remainder的 Integer 的范围

            // 这里不能 step.intValue()==0，因为可能溢出
            if (step.compareTo(BigInteger.ZERO) == 0) {
                expectSliceNumber = remainder.intValue();
            }

            BigInteger[] result = new BigInteger[expectSliceNumber + 1];
            result[0] = left;
            result[expectSliceNumber] = right;

            BigInteger lowerBound;
            BigInteger upperBound = left;
            for (int i = 1; i < expectSliceNumber; i++) {
                lowerBound = upperBound;
                upperBound = lowerBound.add(step);
                upperBound = upperBound.add((remainder.compareTo(BigInteger.valueOf(i)) >= 0)
                        ? BigInteger.ONE : BigInteger.ZERO);
                result[i] = upperBound;
            }

            return result;
        }
    }
}
