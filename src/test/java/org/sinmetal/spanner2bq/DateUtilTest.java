package org.sinmetal.spanner2bq;

import com.google.cloud.Timestamp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class DateUtilTest {

    @Test
    public void testConvertTimestampToDate() {
        Date now = new Date();
        Timestamp timestamp = Timestamp.of(now);
        Date date = DateUtil.convertTimestampToDate(timestamp);
        assertEquals(date.toString(), now.toString());
    }

    @Test
    public void testConvertDateToDate() {
        com.google.cloud.Date from = com.google.cloud.Date.fromYearMonthDay(2018, 6, 18);
        Date to = DateUtil.convertDateToDate(from);
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
        assertEquals("2018-06-18", fmt.format(to));
    }
}
