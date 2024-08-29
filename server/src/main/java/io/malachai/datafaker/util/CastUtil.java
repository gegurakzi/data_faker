package io.malachai.datafaker.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class CastUtil {

    public static String toToken(Object object, String type) {
        switch (type) {
            case "string":
                return "\"" + object + "\"";
            case "int":
            case "long":
            case "double":
                return object.toString();
            case "timestamp":
            case "date":
                return "'" + object + "'";
        }
        return object.toString();
    }

    public static String toToken(String token) {
        return "`" + token + "`";
    }

    public static Object retrieve(LocalDateTime localDateTime, String type) {
        switch (type) {
            case "string":
            case "timestamp":
                return localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            case "date":
                return localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            case "long":
                return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
        return localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

}
