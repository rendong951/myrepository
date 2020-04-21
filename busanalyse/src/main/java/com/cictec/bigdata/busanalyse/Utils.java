package com.cictec.bigdata.busanalyse;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Utils {
    public static Date toDate(String timeStr) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
        return df.parse(timeStr);
    }

    public static long timeDiff(Date time1, Date time2) throws ParseException {

        return time1.getTime() - time2.getTime();

    }
}
