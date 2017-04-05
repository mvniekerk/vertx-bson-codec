package za.co.koperfontein.vertxbsoncodec

import groovy.transform.CompileStatic
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.json.DecodeException
import io.vertx.core.json.EncodeException

import java.sql.Timestamp
import java.util.regex.Pattern

import static za.co.koperfontein.vertxbsoncodec.LE.*

@CompileStatic
class BSONMessageCodec implements MessageCodec<BSONDocument, BSONDocument> {
    
    private static final byte FLOAT               = 0x01.byteValue()
    private static final byte STRING              = 0x02.byteValue()
    private static final byte EMBEDDED_DOCUMENT   = 0X03.byteValue()
    private static final byte ARRAY               = 0X04.byteValue()
    private static final byte BINARY              = 0X05.byteValue()
    private static final byte BINARY_BINARY       = 0X00.byteValue()
    private static final byte BINARY_BINARY_OLD   = 0x02.byteValue()
    private static final byte BINARY_FUNCTION     = 0X01.byteValue()
    private static final byte BINARY_UUID_OLD     = 0X03.byteValue()
    private static final byte BINARY_UUID         = 0X04.byteValue()
    private static final byte BINARY_MD5          = 0X05.byteValue()
    private static final byte BINARY_USERDEFINED  = 0X80.byteValue()
    private static final byte UNDEFINED           = 0X06.byteValue()
    private static final byte OBJECT_ID           = 0X07.byteValue()
    private static final byte BOOLEAN             = 0X08.byteValue()
    private static final byte UTC_DATETIME        = 0X09.byteValue()
    private static final byte NULL                = 0X0A.byteValue()
    private static final byte REGEX               = 0X0B.byteValue()
    private static final byte DBPOINTER           = 0X0C.byteValue()
    private static final byte JSCODE              = 0X0D.byteValue()
    private static final byte SYMBOL              = 0X0E.byteValue()
    private static final byte JSCODE_WS           = 0X0F.byteValue()
    private static final byte INT32               = 0X10.byteValue()
    private static final byte TIMESTAMP           = 0X11.byteValue()
    private static final byte INT64               = 0X12.byteValue()
    private static final byte MINKEY              = 0XFF.byteValue()
    private static final byte MAXKEY              = 0X7F.byteValue()

    private static Map<Class, Closure> encodeMap = [
            (Void)     : { Buffer buffer, String key, Object val ->
                appendKey buffer, key, NULL
            },
            (Double)   : { Buffer buffer, String key, Double val ->
                appendKey buffer, key, FLOAT
                appendDouble buffer, val
            },
            (String)   : { Buffer buffer, String key, String val ->
                appendKey buffer, key, STRING
                appendString buffer, val
            },
            (Map)      : { Buffer buffer, String key, Map<String, ?> val ->
                appendKey buffer, key, EMBEDDED_DOCUMENT
                buffer.appendBuffer encode(val)
            },
            (List)     : { Buffer buffer, String key, List val ->
                appendKey buffer, key, ARRAY
                buffer.appendBuffer encode(val)
            },
            (UUID)     : { Buffer buffer, String key, UUID val ->
                appendKey buffer, key, BINARY
                appendInt buffer, 16
                appendByte buffer, BINARY_UUID
                buffer.appendLong val.mostSignificantBits
                buffer.appendLong val.leastSignificantBits
            },
            (byte[])   : { Buffer buffer, String key, byte[] val ->
                appendKey buffer, key, BINARY
                appendInt buffer, val.length
                appendByte buffer, BINARY_BINARY
                appendBytes buffer, val
            },
            (Buffer)   : { Buffer buffer, String key, Buffer val ->
                appendKey buffer, key, BINARY
                appendInt buffer, val.length()
                appendByte buffer, BINARY_USERDEFINED
                buffer.appendBuffer val
            },
            (MD5)      : { Buffer buffer, String key, MD5 val ->
                appendKey buffer, key, BINARY
                appendInt buffer, val.hash.length
                appendByte buffer, BINARY_MD5
                appendBytes buffer, val.hash
            },
            (ObjectId) : { Buffer buffer, String key, ObjectId val ->
                appendKey buffer, key, OBJECT_ID
                appendBytes buffer, val.bytes
            },
            (Boolean)  : { Buffer buffer, String key, Boolean val ->
                appendKey buffer, key, BOOLEAN
                appendBoolean buffer, val
            },
            (Timestamp): { Buffer buffer, String key, Timestamp val->
                appendKey buffer, key, TIMESTAMP
                appendLong buffer, val.time
            },
            (Date)     : { Buffer buffer, String key, Date val ->
                appendKey buffer, key, UTC_DATETIME
                appendLong buffer, val.time
            },
            (Pattern)  : {Buffer buffer, String key, Pattern val ->
                appendKey buffer, key, REGEX
                appendCString buffer, val.pattern()

                def iFlags = val.flags()

                appendCString buffer, [
                        (Pattern.CASE_INSENSITIVE)          : "i",
                        (Pattern.MULTILINE)                 : "m",
                        (Pattern.DOTALL)                    : "s",
                        (Pattern.UNICODE_CASE)              : "u",
                        (Pattern.COMMENTS)                  : "x",
                        (Pattern.UNICODE_CHARACTER_CLASS)   : "l"
                ].collect { (iFlags & it.key) ? it.value : ""}
                .join("")
            },
            (Integer)  : { Buffer buffer, String key, Integer val ->
                appendKey buffer, key, INT32
                appendInt buffer, val
            },
            (Long)     : { Buffer buffer, String key, Long val ->
                appendKey buffer, key, INT64
                appendLong buffer, val
            },
            (Key)      : { Buffer buffer, String key, Key val ->
                appendKey buffer, key, (val == Key.MIN ? MINKEY : MAXKEY)
            }
    ] as Map<Class, Closure>

    @Override
    void encodeToWire(Buffer buffer, BSONDocument bsonDocument) {
        def base = buffer.length()
        appendInt buffer, 0
        bsonDocument.entrySet().each { encode buffer, it.key, it.value }
        setInt buffer, base, buffer.length() + 1 - base
        appendByte buffer, 0x00.byteValue()
    }
    
    private static void encode(Buffer buffer, String key, Object val) {
        def c =  encodeMap[val?.class ?: Void]
        c = c ?: encodeMap.find { Map.Entry<Class, Closure> i -> i.key.isAssignableFrom(val.class)}?.value
        if (!c) {
            throw new EncodeException("Dont know how to encode ${val.class.name}")
        }
        c buffer, key, val
    }

    private static Buffer encode(Map<String, ?> j) {
        def buffer = Buffer.buffer()
        appendInt buffer, 0
        j.each { encode buffer, it.key, it.value }
        setInt buffer, 0, buffer.length() + 1
        appendByte buffer, (byte)0x0
        buffer
    }

    private static Buffer encode(List<?> list) {
        def buffer = Buffer.buffer()
        appendInt buffer, 0
        list.eachWithIndex { val, i -> encode buffer, i.toString(), val }
        setInt buffer, 0, buffer.length()+1
        appendByte buffer, (byte)0x0
        buffer
    }

    private static BSONDocument decodeDocument(Buffer buffer, int pos) {
        def length = pos + getInt(buffer, pos) - 1
        pos += 4
        def doc = new BSONDocument()
        while (pos < length) {
            def t = getByte buffer, pos
            pos++
            def key = getCString buffer, pos
            pos += key.length() + 1

            switch (t) {
                case FLOAT:
                    doc[key] = getDouble(buffer, pos)
                    pos += 8
                    break
                case STRING:
                    def utfLength = getInt buffer, pos
                    pos += 4
                    doc[key] = getString(buffer, pos, utfLength -1)
                    pos += utfLength
                    break
                case EMBEDDED_DOCUMENT:
                    def docLen = getInt buffer, pos
                    doc[key] = decodeDocument(buffer, pos)
                    pos += docLen
                    break
                case ARRAY:
                    def arrLen = getInt buffer, pos
                    doc[key] = decodeList(buffer, pos)
                    pos += arrLen
                    break
                case BINARY:
                    def binLen = getInt buffer, pos
                    pos += 4
                    def binType = getByte buffer, pos
                    pos++
                    switch (binType) {
                        case BINARY_BINARY:
                            doc[key] = getBytes buffer, pos, binLen
                            pos += binLen
                            break
                        case BINARY_FUNCTION:
                            throw new DecodeException("Not implemented")
                        case BINARY_BINARY_OLD:
                            def oldBinLen = getInt buffer, pos
                            pos += 4
                            doc[key] = getBytes(buffer, pos, oldBinLen)
                            pos += binLen
                            break
                        case BINARY_UUID_OLD:
                            throw new DecodeException("Not implemented")
                        case BINARY_UUID:
                            def mostSignificant = buffer.getLong pos
                            pos += 8
                            def leasSignificant = buffer.getLong pos
                            pos += 8
                            doc[key] = new UUID(mostSignificant, leasSignificant)
                            break
                        case BINARY_MD5:
                            def byte[] md5 = getBytes buffer, pos, binLen
                            doc.put key, {md5}
                            pos += binLen
                            break
                        case BINARY_USERDEFINED:
                            doc[key] = buffer.getBuffer pos, pos+binLen
                            pos += binLen
                            break
                    }
                    break
                case UNDEFINED:
                    break
                case OBJECT_ID:
                    doc[key] = new ObjectId(getBytes(buffer, pos, 12))
                    pos += 12
                    break
                case BOOLEAN:
                    doc[key] = getBoolean buffer, pos
                    pos++
                    break
                case UTC_DATETIME:
                    doc[key] = new Date(getLong(buffer, pos))
                    pos += 8
                    break
                case NULL:
                    doc[key] = null
                    break
                case REGEX:
                    def regex = getCString buffer, pos
                    pos += regex.length() + 1
                    def options = getCString buffer, pos
                    pos += options.length() + 1

                    doc[key] = Pattern.compile regex, (options.collect { c ->
                        switch (c) {
                            case "m":
                                Pattern.MULTILINE
                                break
                            case "s":
                                Pattern.DOTALL
                                break
                            case "u":
                                Pattern.UNICODE_CASE
                                break
                            case "x":
                                Pattern.COMMENTS
                                break
                            case "i":
                                Pattern.UNICODE_CHARACTER_CLASS
                                break
                            default:
                                0
                        }
                    }.inject(0, {ret, i -> i + ret}) as int)
                    break
                case DBPOINTER:
                case JSCODE:
                case SYMBOL:
                case JSCODE_WS:
                    throw new DecodeException("Not implemented")
                case INT32:
                    doc[key] = getInt buffer, pos
                    pos += 4
                    break
                case TIMESTAMP:
                    doc[key] = new Timestamp(getLong(buffer, pos))
                    pos += 8
                    break
                case INT64:
                    doc[key] = getLong buffer, pos
                    pos += 8
                    break
                case MINKEY:
                    doc[key] = Key.MIN
                    break
                case MAXKEY:
                    doc[key] = Key.MAX
            }
        }
        doc
    }

    private static List<?> decodeList(Buffer buffer, int pos) {
        def length = pos + getInt(buffer, pos) - 1
        pos += 4

        def list = new LinkedList()
        while (pos < length) {
            def type = getByte buffer, pos
            ++pos
            def key = getCString buffer, pos
            pos += key.length() + 1
            switch (type) {
                case FLOAT:
                    list.add key.toInteger(), getDouble(buffer, pos)
                    pos += 8
                    break
                case STRING:
                    def utfLength = getInt buffer, pos
                    pos += 4
                    list.add key.toInteger(), getString(buffer, pos, utfLength-1)
                    pos += utfLength
                    break
                case EMBEDDED_DOCUMENT:
                    def docLen = getInt(buffer, pos)
                    list.add key.toInteger(), decodeDocument(buffer, pos)
                    pos += docLen
                    break
                case ARRAY:
                    def arrLen = getInt(buffer, pos)
                    list.add key.toInteger(), decodeList(buffer, pos)
                    pos += arrLen
                    break
                case BINARY:
                    def binLen = getInt(buffer, pos)
                    pos += 4
                    def binType = getByte buffer, pos
                    pos++
                    switch (binType) {
                        case BINARY_BINARY:
                            list.add key.toInteger(), getBytes(buffer, pos, binLen)
                            pos += binLen
                            break

                        case BINARY_FUNCTION:
                            throw new DecodeException("Not implemented")
                        case BINARY_BINARY_OLD:
                            def oldBinLen = getInt(buffer, pos)
                            pos += 4
                            list.add key.toInteger(), getBytes(buffer, pos, oldBinLen)
                            pos += binLen
                            break
                        case BINARY_UUID_OLD:
                            throw new DecodeException("Not implemented")
                        case BINARY_UUID:
                            def most = buffer.getLong(pos)
                            pos += 8
                            def least = buffer.getLong(pos)
                            pos +- 8
                            list.add key.toInteger(), new UUID(most, least)
                            break
                        case BINARY_MD5:
                            def md5 = getBytes(buffer, pos, binLen)
                            list.add key.toInteger(), [getHash: {md5}] as MD5
                            pos += binLen
                            break
                        case BINARY_USERDEFINED:
                            list.add key.toInteger(), buffer.getBuffer(pos, pos + binLen)
                            pos += binLen
                            break
                    }
                    break
                case UNDEFINED:
                    break
                case OBJECT_ID:
                    list.add key.toInteger(), new ObjectId(getBytes(buffer, pos, 12))
                    pos += 12
                    break
                case BOOLEAN:
                    list.add key.toInteger(), getBoolean(buffer, pos)
                    pos++
                    break
                case UTC_DATETIME:
                    list.add key.toInteger(), new Date(getLong(buffer, pos))
                    pos += 8
                    break
                case NULL:
                    list.add key.toInteger(), null
                    break
                case REGEX:
                    def regex = getCString buffer, pos
                    pos += regex.length() + 1
                    def options = getCString buffer, pos
                    pos += options.length() + 1
                    int flags = options.collect({
                        switch (it) {
                            case 'i':
                                Pattern.CASE_INSENSITIVE
                                break
                            case 'm':
                                Pattern.MULTILINE
                                break
                            case 's':
                                Pattern.DOTALL
                                break
                            case 'u':
                                Pattern.UNICODE_CASE
                                break
                            case 'x':
                                Pattern.COMMENTS
                                break
                            case 'l':
                                Pattern.UNICODE_CHARACTER_CLASS
                                break
                            default:
                                0
                        }
                    }).sum() as int
                    list.add key.toInteger(), Pattern.compile(regex, flags)
                    break
                case DBPOINTER:
                case JSCODE:
                case SYMBOL:
                case JSCODE_WS:
                    throw new DecodeException("Not implemented")
                case INT32:
                    list.add key.toInteger(), getInt(buffer,pos)
                    pos += 4
                    break
                case TIMESTAMP:
                    list.add key.toInteger(), new Timestamp(getLong(buffer, pos))
                    pos += 8
                    break
                case INT64:
                    list.add key.toInteger(), getLong(buffer, pos)
                    pos += 8
                    break
                case MINKEY:
                    list.add key.toInteger(), Key.MIN
                    break
                case MAXKEY:
                    list.add key.toInteger(), Key.MAX
                    break
            }
        }
        list
    }

    @Override
    BSONDocument decodeFromWire(int pos, Buffer buffer) {
        buffer ? decodeDocument(buffer, pos) : null
    }

    @Override
    String name() {
        "BSON"
    }

    @Override
    BSONDocument transform(BSONDocument bsonDocument) {
        bsonDocument
    }

    @Override
    byte systemCodecID() {
        -1
    }
}
