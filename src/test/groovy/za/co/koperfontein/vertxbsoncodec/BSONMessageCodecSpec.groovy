package za.co.koperfontein.vertxbsoncodec

import io.vertx.core.buffer.Buffer
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.regex.Pattern

class BSONMessageCodecSpec extends Specification {
    @Shared BSONMessageCodec codec = new BSONMessageCodec()

    def "Test old binary"() {
        setup:
        def bson = [
                //Length
                0x14, 0x00, 0x00, 0x00,
                //Data
                0x05, (char)'_', 0x00, 0x07, 0x00, 0x00, 0x00, 0x02, 0x03, 0x00, 0x00, 0x00, (char)'o', (char)'l', (char)'d',
                //End
                0x000
        ] as byte[]
        def doc = codec.decodeFromWire(0, Buffer.buffer(bson))

        expect:
        doc['_'] == ['o','l','d']
    }

    def "Test empty map"() {
        setup:
        def doc = new BSONDocument()
        def buffer = Buffer.buffer()

        when: 'Encode'
        codec.encodeToWire(buffer, doc)
        and: 'Get wire bytes'
        def expected = buffer.bytes
        then: 'Bytes should be correct'
        expected == [0x5, 0x0, 0x0, 0x0, 0x0] as byte[]

        when: 'Decode'
        def dec = codec.decodeFromWire(0, Buffer.buffer(expected))
        then: 'It should be the same'
        dec == doc
    }

    def "Sub document boolean"() {
        setup:
        def doc = new BSONDocument()
        def subDoc = new BSONDocument()
        subDoc['a'] = true
        doc['_'] = subDoc
        def buffer = Buffer.buffer()

        when: 'Encode'
        codec.encodeToWire buffer, doc
        and: 'Get wire bytes'
        def val = buffer.bytes
        def expected = [
                // length
                0x11, 0x00, 0x00, 0x00,
                0x03, (char)'_', 0x00, 0x09, 0x00, 0x00, 0x00, 0x08, (char)'a', 0x00, 0x01, 0x00,
                // end
                0x00
        ] as byte[]

        then: 'The bytes should be correct'
        val == expected

        when: 'Decode from wire'
        def dec = codec.decodeFromWire 0, Buffer.buffer(expected)
        then: 'The value should be correct'
        dec == doc
    }

    @Unroll
    def "Type: #valType Val: #val"() {
        setup:
        def doc = new BSONDocument()
        doc['_'] = val
        def buffer = Buffer.buffer()
        codec.encodeToWire(buffer, doc)

        expect:
        buffer.bytes == expected

        where:
        valType     | val          |   expected
        "boolean"   | true         |   [
                                                                // length
                                                                0x09, 0x00, 0x00, 0x00,
                                                                0x08, (char)'_', 0x00, 0x01,
                                                                // end
                                                                0x00
                                                            ] as byte[]
        "boolean"   | false                             |   [
                                                                // length
                                                                0x09, 0x00, 0x00, 0x00,
                                                                0x08, (char)'_', 0x00, 0x00,
                                                                // end
                                                                0x00
                                                            ] as byte[]
        "String"    | "yo"                              |   [
                                                                // length
                                                                0x0f, 0x00, 0x00, 0x00,
                                                                0x02, (char)'_', 0x00, 0x03, 0x00, 0x00, 0x00, (char)'y', (char)'o', 0x00,
                                                                // end
                                                                0x00
                                                            ] as byte[]
        "double"    | 5.05d                             |   [
                                                                // length
                                                                0x10, 0x0, 0x0, 0x0,
                                                                0x01, (char)'_', 0x00, (char)'3', (char)'3', (char)'3', (char)'3', (char)'3', (char)'3', 0x14, (char)'@',
                                                                // end
                                                                0x00
                                                            ] as byte[]
        "null"      | null                              |   [
                                                                // length
                                                                0x08, 0x00, 0x00, 0x00,
                                                                0x0a, (char)'_', 0x00,
                                                                // end
                                                                0x00
                                                            ] as byte[]
        "regex"     | Pattern.compile("ab")       |  [
                                                                // length
                                                                0x0c, 0x00, 0x00, 0x00,
                                                                0x0b, (char)'_', 0x00, (char)'a', (char)'b', 0x00, 0x00,
                                                                // end
                                                                0x00
                                                            ] as byte[]
        "date"      | new Date(258)                |  [
                                                                // length
                                                                0x10, 0x00, 0x00, 0x00,
                                                                0x09, (char)'_', 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                                                // end
                                                                0x00
                                                            ] as byte[]
        "binary"    | [(char)'y', (char)'o'] as byte[]  |   [
                                                                // length
                                                                0x0f, 0x00, 0x00, 0x00,
                                                                0x05, (char)'_', 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, (char)'y', (char)'o',
                                                                // end
                                                                0x00
                                                            ] as byte[]
        "int32"     | 258                               |   [
                                                                // length
                                                                0x0c, 0x00, 0x00, 0x00,
                                                                0x10, (char)'_', 0x00, 0x02, 0x01, 0x00, 0x00,
                                                                // end
                                                                0x00
                                                            ] as byte[]
        "int64"     | 258l                              |   [
                                                                // length
                                                                0x10, 0x00, 0x00, 0x00,
                                                                0x12, (char)'_', 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                                                // end
                                                                0x00
                                                            ] as byte[]
        "int64 << 2"| (258l << 32) |   [
                                                                // length
                                                                0x10, 0x00, 0x00, 0x00,
                                                                0x12, (char)'_', 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00, 0x00,
                                                                // end
                                                                0x00
                                                            ] as byte[]
        "MIN key"   | Key.MIN      |   [
                                                                // length
                                                                0x08, 0x00, 0x00, 0x00,
                                                                (byte) 0xff, (char)'_', 0x00,
                                                                // end
                                                                0x00
                                                            ] as byte[]
        "MAX key"   | Key.MAX      |   [
                                                                // length
                                                                0x08, 0x00, 0x00, 0x00,
                                                                0x7f, (char)'_', 0x00,
                                                                // end
                                                                0x00
                                                            ] as byte[]
    }

}
