package org.sinmetal.spanner2bq;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class SpannerUtilTest {

    @Test
    public void testGetBigQueryColumnType() throws Exception {
        assertEquals(SpannerUtil.getBigQueryColumnType("STRING(MAX)"), "STRING");
        assertEquals(SpannerUtil.getBigQueryColumnType("INT64"), "INT64");
        assertEquals(SpannerUtil.getBigQueryColumnType("FLOAT64"), "FLOAT64");
        assertEquals(SpannerUtil.getBigQueryColumnType("BOOL"), "BOOL");
        assertEquals(SpannerUtil.getBigQueryColumnType("TIMESTAMP"), "TIMESTAMP");
        assertEquals(SpannerUtil.getBigQueryColumnType("ARRAY<STRING(MAX)>"), "ARRAY<STRING>");
    }
}
