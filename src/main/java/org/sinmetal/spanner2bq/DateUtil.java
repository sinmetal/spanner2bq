package org.sinmetal.spanner2bq;

import com.google.cloud.Timestamp;

import java.util.Calendar;
import java.util.Date;

public class DateUtil {

    public static Date convertTimestampToDate(Timestamp timestamp) {
        return new Date(timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000);
    }

    public static Date convertDateToDate(com.google.cloud.Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(date.getYear(), date.getMonth() - 1, date.getDayOfMonth());
        return calendar.getTime();
    }
}
