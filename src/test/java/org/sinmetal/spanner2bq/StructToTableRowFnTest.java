package org.sinmetal.spanner2bq;

import com.google.cloud.Timestamp;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.util.Date;

@RunWith(JUnit4.class)
public class StructToTableRowFnTest {

    @Test
    public void testTimestampFormat() {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
        Date date = new Date();
        long v = date.getTime();
        Date newDate = new Date(1528871510197L);
        assertEquals(fmt.format(newDate), "2018-06-13 15:31:50.000197");
    }

    @Test
    public void testDateFormat() {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        long v = date.getTime();
        Date newDate = new Date(1528871510197L);
        assertEquals(fmt.format(newDate), "2018-06-13");
    }

    @Test
    public void testTimestampToDate() {
        Timestamp now = Timestamp.now();
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        long v = date.getTime();
        System.out.print(v);
        System.out.print("\n");

        System.out.print(now.getSeconds() + "\n");
        System.out.print(now.getNanos() / 1000000 + "\n");
        System.out.print(now.getSeconds() * 1000 + now.getNanos() / 1000000);
        Date newDate = new Date(now.getSeconds());
        assertEquals(fmt.format(newDate), "2018-06-13");
    }
}
