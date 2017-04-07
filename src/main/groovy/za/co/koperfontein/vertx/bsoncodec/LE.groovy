package za.co.koperfontein.vertx.bsoncodec

import groovy.transform.CompileStatic
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.DecodeException
import io.vertx.core.json.EncodeException

import java.nio.charset.Charset

@CompileStatic
class LE {
    private static final Charset UTF8 = Charset.forName "UTF-8"

    static appendBoolean    (Buffer buffer, boolean val)    { buffer.appendByte (val ? (byte)0x01 : (byte)0x00) }
    static appendByte       (Buffer buffer, byte val)       { buffer.appendByte (val) }
    static appendShort      (Buffer buffer, short val)      { buffer.appendShort(Short.reverseBytes(val))}
    static appendInt        (Buffer buffer, int val)        { buffer.appendInt  (Integer.reverseBytes(val))}
    static appendFloat      (Buffer buffer, float val)      { buffer.appendInt  (Float.floatToRawIntBits(val))}
    static appendDouble     (Buffer buffer, double val)     { appendLong(buffer, Double.doubleToRawLongBits(val))}
    static appendLong       (Buffer buffer, long value)     { buffer.appendLong(Long.reverseBytes(value)) }
    static appendBytes      (Buffer buffer, byte[] val)     { buffer.appendBytes(val)}
    static appendChar       (Buffer buffer, char val)       { buffer.appendByte((byte)(val.charValue()))}
    static appendCString    (Buffer buffer, String val)     {
        def b = val.getBytes "UTF-8"
        if (b.any {it == '\0'}) {
            throw new EncodeException("Key: '$val' is not a CString")
        }
        buffer.appendBytes(b)
        buffer.appendByte((byte)0x00)
    }
    static appendString     (Buffer buffer, String val)     {
        def b = val.getBytes "UTF-8"
        appendInt buffer, b.length+1
        buffer.appendBytes b
        buffer.appendByte((byte)0x0)
    }
    static appendKey        (Buffer buffer, String key, byte index) {
        appendByte buffer, index
        appendCString buffer, key
    }

    static setByte          (Buffer buffer, int pos, byte val)      { buffer.setByte pos, val}
    static setBytes         (Buffer buffer, int pos, byte[] val)    { buffer.setBytes pos, val}
    static setShort         (Buffer buffer, int pos, short val)     { buffer.setShort pos, Short.reverseBytes(val)}
    static setInt           (Buffer buffer, int pos, int val)       { buffer.setInt pos, Integer.reverseBytes(val)}
    static setLong          (Buffer buffer, int pos, long val)      { buffer.setLong pos, Long.reverseBytes(val)}
    static setFloat         (Buffer buffer, int pos, float val)     { setInt buffer, pos, Float.floatToRawIntBits(val)}
    static setDouble        (Buffer buffer, int pos, double val)    { setLong buffer, pos, Double.doubleToRawLongBits(val)}

    static getBoolean       (Buffer buffer, int pos) {
        def b = buffer.getByte pos
        if (b == (byte)0x0) { return false }
        if (b == (byte)0x1) { return true }
        throw new DecodeException("$b is not a valid boolean value")
    }
    static byte     getByte     (Buffer buffer, int pos)                { buffer.getByte pos}
    static byte[]   getBytes    (Buffer buffer, int pos, int length)    { buffer.getBytes pos, pos+length}
    static int      getInt      (Buffer buffer, int pos)                { Integer.reverseBytes(buffer.getInt(pos))}
    static long     getLong     (Buffer buffer, int pos)                { Long.reverseBytes(buffer.getLong(pos))}
    static float    getFloat    (Buffer buffer, int pos)                { Float.intBitsToFloat(getInt(buffer, pos))}
    static double   getDouble   (Buffer buffer, int pos)                { Double.longBitsToDouble(getLong(buffer, pos))}
    static String   getCString  (Buffer buffer, int pos)                {
        def end = pos
        while (buffer.getByte(end) != (byte)0x0) {
            end++
        }
        buffer.getString(pos, end, "UTF-8")
    }
    static String   getString   (Buffer buffer, int pos, int length)    { buffer.getString pos, pos+length, "UTF-8"}
}
