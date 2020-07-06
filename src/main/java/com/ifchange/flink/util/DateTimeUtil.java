package com.ifchange.flink.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DateTimeUtil {

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * use java8 time
     * 返回毫秒
     * @param timeStr time
     * @return 毫秒
     * @throws Exception
     */
    public static long parseStrToLongMill(String timeStr) throws Exception {
        try {
            LocalDateTime localDateTime = LocalDateTime.parse(timeStr, dtf);
            return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        } catch (Exception e) {
            throw new Exception("time parse error" + e.getMessage());
        }
    }

    /**
     * use java8 time
     * 返回秒
     * @param timeStr time
     * @return 秒
     * @throws Exception
     */
    public static long parseStrToLongSec(String timeStr) throws Exception {
        try {
            LocalDateTime localDateTime = LocalDateTime.parse(timeStr, dtf);
            return localDateTime.toEpochSecond(ZoneOffset.of("+8"));
        } catch (Exception e) {
            throw new Exception("time parse error" + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception{
        String str="2018-09-14 11:12:21";
        long mill = DateTimeUtil.parseStrToLongMill(str);
        long sec = DateTimeUtil.parseStrToLongSec(str);
        System.out.println(mill);
        System.out.println(sec);
    }
}
