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

package com.dtstack.chunjun.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;

public class ValueUtil {

    public static Integer getInt(Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof String) {
            return Integer.valueOf((String) obj);
        } else {
            try {
                Method method = obj.getClass().getMethod("intValue");
                return (int) method.invoke(obj);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException("Unable to convert " + obj + " into Interger", e);
            }
        }
    }

    public static Long getLongVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Long.valueOf((String) obj);
        } else if (obj instanceof Long) {
            return (Long) obj;
        } else if (obj instanceof Integer) {
            return Long.valueOf(obj.toString());
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).longValue();
        } else if (obj instanceof BigInteger) {
            return ((BigInteger) obj).longValue();
        }
        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Long.");
    }

    public static Long getLongVal(Object obj, long defaultVal) {
        if (obj == null) {
            return defaultVal;
        }

        return getLongVal(obj);
    }

    public static Integer getIntegerVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Integer.valueOf((String) obj);
        } else if (obj instanceof Integer) {
            return (Integer) obj;
        } else if (obj instanceof Long) {
            return ((Long) obj).intValue();
        } else if (obj instanceof Double) {
            return ((Double) obj).intValue();
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).intValue();
        } else if (obj instanceof BigInteger) {
            return ((BigInteger) obj).intValue();
        }

        throw new RuntimeException(
                "not support type of " + obj.getClass() + " convert to Integer.");
    }

    public static Integer getIntegerVal(Object obj, int defaultVal) {
        if (obj == null) {
            return defaultVal;
        }

        return getIntegerVal(obj);
    }

    public static Float getFloatVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Float.valueOf((String) obj);
        } else if (obj instanceof Float) {
            return (Float) obj;
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).floatValue();
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Float.");
    }

    public static Float getFloatVal(Object obj, float defaultVal) {
        if (obj == null) {
            return defaultVal;
        }

        return getFloatVal(obj);
    }

    public static Double getDoubleVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Double.valueOf((String) obj);
        } else if (obj instanceof Float) {
            return ((Float) obj).doubleValue();
        } else if (obj instanceof Double) {
            return (Double) obj;
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).doubleValue();
        } else if (obj instanceof Integer) {
            return ((Integer) obj).doubleValue();
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Double.");
    }

    public static Double getDoubleVal(Object obj, double defaultVal) {
        if (obj == null) {
            return defaultVal;
        }

        return getDoubleVal(obj);
    }

    public static Boolean getBooleanVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Boolean.valueOf((String) obj);
        } else if (obj instanceof Boolean) {
            return (Boolean) obj;
        }

        throw new RuntimeException(
                "not support type of " + obj.getClass() + " convert to Boolean.");
    }

    public static Boolean getBooleanVal(Object obj, boolean defaultVal) {
        if (obj == null) {
            return defaultVal;
        }

        return getBooleanVal(obj);
    }

    public static String getStringVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return (String) obj;
        }

        return obj.toString();
    }

    public static Byte getByteVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Byte.valueOf((String) obj);
        } else if (obj instanceof Byte) {
            return (Byte) obj;
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Byte.");
    }

    public static Short getShortVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Short.valueOf((String) obj);
        } else if (obj instanceof Short) {
            return (Short) obj;
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Short.");
    }

    public static BigDecimal getBigDecimalVal(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof String) {
            return new BigDecimal((String) obj);
        } else if (obj instanceof BigDecimal) {
            return (BigDecimal) obj;
        } else if (obj instanceof BigInteger) {
            return new BigDecimal((BigInteger) obj);
        } else if (obj instanceof Number) {
            return BigDecimal.valueOf(((Number) obj).doubleValue());
        }
        throw new RuntimeException(
                "not support type of " + obj.getClass() + " convert to BigDecimal.");
    }

    public static Timestamp getTimestampVal(Object obj) {
        return DateUtil.columnToTimestamp(obj, null);
    }
}
