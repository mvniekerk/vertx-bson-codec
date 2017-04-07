package za.co.koperfontein.vertx.bsoncodec

import groovy.transform.CompileStatic

import java.text.DateFormat
import java.text.ParseException
import java.text.SimpleDateFormat

@CompileStatic
class ThreadLocalUTCDateFormat extends ThreadLocal<DateFormat> {

    String format(Date date)                    { get().format date }
    String format(Object val)                   { get().format val }
    Date parse(String t) throws ParseException  { get().parse t }

    @Override
    protected DateFormat initialValue() {
        def df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        df.timeZone = TimeZone.getTimeZone("UTC")
        df
    }
}
