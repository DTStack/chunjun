//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.hadoop.hive.common.type;

import org.apache.hive.common.util.DateParser;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class HiveDate extends Date {
    private static boolean dateShowTime = true;
    private java.util.Date privateDate = null;
    private static final long serialVersionUID = -261750473771622491L;
    private static final ThreadLocal<TimeZone> LOCAL_TIMEZONE =
            new ThreadLocal<TimeZone>() {
                protected TimeZone initialValue() {
                    return Calendar.getInstance().getTimeZone();
                }
            };

    public HiveDate() {
        super(System.currentTimeMillis());
        this.privateDate = new java.util.Date(this.getTime());
    }

    //    private static boolean isDateShowTime() {
    //        String confValue = GlobalHiveConf.get(ConfVars.INCEPTOR_DATE_SHOW_TIME.varname);
    //        return confValue.toLowerCase().equalsIgnoreCase("true");
    //    }

    /** @deprecated */
    @Deprecated
    public HiveDate(int year, int month, int day) {
        super(year, month, day);
        this.privateDate = new java.util.Date(this.getTime());
    }

    /** @deprecated */
    @Deprecated
    public HiveDate(int year, int month, int day, int hrs, int min) {
        super(0L);
        this.privateDate = new java.util.Date(year, month, day, hrs, min);
        this.setTime(this.privateDate.getTime());
    }

    /** @deprecated */
    @Deprecated
    public HiveDate(int year, int month, int day, int hrs, int min, int sec) {
        super(0L);
        this.privateDate = new java.util.Date(year, month, day, hrs, min, sec);
        this.setTime(this.privateDate.getTime());
    }

    public HiveDate(long date) {
        super(date);
        this.privateDate = new java.util.Date(date);
    }

    public java.util.Date getPrivateDate() {
        return this.privateDate;
    }

    public void setPrivateDate(java.util.Date privateDate) {
        this.privateDate = privateDate;
    }

    public void setTime(long date) {
        super.setTime(date);
        this.privateDate.setTime(date);
    }

    public static HiveDate valueOf(String s) {
        int dateStrLen = s.length();
        HiveDate d = null;
        Calendar calendar = Calendar.getInstance();
        String hourStr = null;
        if (s == null) {
            throw new IllegalArgumentException("String " + s + " can not be converted to Date");
        } else {
            if (s.indexOf(32) != -1) {
                String[] dateStr = s.split("\\s+");
                if (dateStr.length < 1 || dateStr.length > 2) {
                    throw new IllegalArgumentException(
                            "String " + s + " can not be converted to Date");
                }

                if (dateStr.length == 2) {
                    s = dateStr[0];
                    hourStr = dateStr[1];
                }

                dateStrLen = dateStr[0].length();
            }

            int firstDash = s.indexOf(45);
            int secondDash = s.indexOf(45, firstDash + 1);
            String mm;
            String dd;
            String day;
            DateParser dateParser;
            if (firstDash == 2 && secondDash == 6 && dateStrLen == 9) {
                d = new HiveDate();
                dateParser = new DateParser(new SimpleDateFormat("dd-MMM-yy"));
                if (!dateParser.parseDate(s, d, false)) {
                    throw new IllegalArgumentException(
                            "String " + s + " can not be converted to Date");
                }

                if (d != null && hourStr != null) {
                    calendar.setTime(d);
                    mm = String.valueOf(calendar.get(1));
                    dd = String.valueOf(calendar.get(2) + 1);
                    day = String.valueOf(calendar.get(5));
                    d = getHiveDate(mm, dd, day, hourStr);
                }
            } else if (firstDash == 2 && secondDash == 6 && dateStrLen == 11) {
                d = new HiveDate();
                dateParser = new DateParser(new SimpleDateFormat("dd-MMM-yyyy"));
                if (!dateParser.parseDate(s, d, false)) {
                    throw new IllegalArgumentException(
                            "String " + s + " can not be converted to Date");
                }

                if (d != null && hourStr != null) {
                    calendar.setTime(d);
                    mm = String.valueOf(calendar.get(1));
                    dd = String.valueOf(calendar.get(2) + 1);
                    day = String.valueOf(calendar.get(5));
                    d = getHiveDate(mm, dd, day, hourStr);
                }
            } else {
                String yyyy;
                if (firstDash > 0 && secondDash > 0 && secondDash < s.length() - 1) {
                    yyyy = s.substring(0, firstDash);
                    mm = s.substring(firstDash + 1, secondDash);
                    dd = s.substring(secondDash + 1);
                    d = getHiveDate(yyyy, mm, dd, hourStr);
                } else if (s.length() == 8 && hourStr == null) {
                    yyyy = s.substring(0, 4);
                    mm = s.substring(4, 6);
                    dd = s.substring(6, 8);
                    d = getHiveDate(yyyy, mm, dd, (String) null);
                }
            }

            if (d == null) {
                throw new IllegalArgumentException("String " + s + " can not be converted to Date");
            } else {
                return d;
            }
        }
    }

    private static HiveDate getHiveDate(String yyyy, String mm, String dd, String hourStr) {
        HiveDate d = null;
        if (yyyy.length() == 4
                && mm.length() > 0
                && mm.length() <= 2
                && dd.length() <= 2
                && dd.length() > 0) {
            int year = Integer.parseInt(yyyy);
            int month = Integer.parseInt(mm);
            int day = Integer.parseInt(dd);
            if (month >= 1 && month <= 12) {
                int maxDays = 31;
                switch (month) {
                    case 2:
                        if ((year % 4 != 0 || year % 100 == 0) && year % 400 != 0) {
                            maxDays = 28;
                        } else {
                            maxDays = 29;
                        }
                    case 3:
                    case 5:
                    case 7:
                    case 8:
                    case 10:
                    default:
                        break;
                    case 4:
                    case 6:
                    case 9:
                    case 11:
                        maxDays = 30;
                }

                if (day >= 1 && day <= maxDays) {
                    if (hourStr == null) {
                        d = new HiveDate(year - 1900, month - 1, day);
                    } else {
                        int firstColon = hourStr.indexOf(58);
                        int secondColon = hourStr.indexOf(58, firstColon + 1);
                        int period = hourStr.indexOf(46, secondColon + 1);
                        if (firstColon > 0
                                && secondColon > 0
                                && secondColon < hourStr.length() - 1) {
                            String hh = hourStr.substring(0, firstColon);
                            String mM = hourStr.substring(firstColon + 1, secondColon);
                            String ss = hourStr.substring(secondColon + 1);
                            String ms = null;
                            if (period > 0) {
                                ss = hourStr.substring(secondColon + 1, period);
                                ms = hourStr.substring(period + 1).trim();
                            }

                            int millis;
                            if (hh.length() == 2 && mM.length() == 2 && ss.length() == 2) {
                                int hour = Integer.parseInt(hh);
                                millis = Integer.parseInt(mM);
                                int second = Integer.parseInt(ss);
                                if (month >= 1
                                        && month <= 12
                                        && day >= 1
                                        && day <= 31
                                        && hour >= 0 & hour <= 23
                                        && millis >= 0
                                        && millis <= 59
                                        && second >= 0
                                        && second <= 59) {
                                    d =
                                            new HiveDate(
                                                    year - 1900,
                                                    month - 1,
                                                    day,
                                                    hour,
                                                    millis,
                                                    second);
                                }
                            }

                            if (ms != null && !ms.isEmpty()) {
                                char[] msList = new char[] {'0', '0', '0'};

                                for (millis = 0; millis < 3; ++millis) {
                                    if (millis < ms.length()) {
                                        msList[millis] = ms.charAt(millis);
                                    }
                                }

                                millis = Integer.valueOf(String.valueOf(msList));
                                if (millis < 999) {
                                    d.setTime(d.getTime() + (long) millis);
                                }
                            }
                        }
                    }
                }
            }
        }

        return d;
    }

    public String toString() {
        Calendar c = Calendar.getInstance((TimeZone) LOCAL_TIMEZONE.get());
        c.setTime(this);
        int year = c.get(1);
        int month = c.get(2) + 1;
        int day = c.get(5);
        int hour = c.get(11);
        int minute = c.get(12);
        int second = c.get(13);
        char[] buf;
        if ((hour != 0 || minute != 0 || second != 0) && dateShowTime) {
            buf = "2000-00-00 00:00:00".toCharArray();
            buf[0] = Character.forDigit(year / 1000, 10);
            buf[1] = Character.forDigit(year / 100 % 10, 10);
            buf[2] = Character.forDigit(year / 10 % 10, 10);
            buf[3] = Character.forDigit(year % 10, 10);
            buf[5] = Character.forDigit(month / 10, 10);
            buf[6] = Character.forDigit(month % 10, 10);
            buf[8] = Character.forDigit(day / 10, 10);
            buf[9] = Character.forDigit(day % 10, 10);
            buf[11] = Character.forDigit(hour / 10, 10);
            buf[12] = Character.forDigit(hour % 10, 10);
            buf[14] = Character.forDigit(minute / 10, 10);
            buf[15] = Character.forDigit(minute % 10, 10);
            buf[17] = Character.forDigit(second / 10, 10);
            buf[18] = Character.forDigit(second % 10, 10);
            return new String(buf);
        } else {
            buf = "2000-00-00".toCharArray();
            buf[0] = Character.forDigit(year / 1000, 10);
            buf[1] = Character.forDigit(year / 100 % 10, 10);
            buf[2] = Character.forDigit(year / 10 % 10, 10);
            buf[3] = Character.forDigit(year % 10, 10);
            buf[5] = Character.forDigit(month / 10, 10);
            buf[6] = Character.forDigit(month % 10, 10);
            buf[8] = Character.forDigit(day / 10, 10);
            buf[9] = Character.forDigit(day % 10, 10);
            return new String(buf);
        }
    }

    public int getHours() {
        return this.privateDate.getHours();
    }

    public int getMinutes() {
        return this.privateDate.getMinutes();
    }

    public int getSeconds() {
        return this.privateDate.getSeconds();
    }

    public void setHours(int i) {
        this.privateDate.setHours(i);
        this.setTime(this.privateDate.getTime());
    }

    public void setMinutes(int i) {
        this.privateDate.setMinutes(i);
        this.setTime(this.privateDate.getTime());
    }

    public void setSeconds(int i) {
        this.privateDate.setSeconds(i);
        this.setTime(this.privateDate.getTime());
    }

    public static void main(String[] args) {
        System.out.println((new java.util.Date()).getYear());
        new HiveDate(System.currentTimeMillis());
        System.out.println(valueOf("2015-01-03 16:12:11"));
        System.out.println(valueOf("2015-01-03"));
    }
}
