package za.co.koperfontein.vertxbsoncodec

import io.vertx.core.buffer.Buffer
import spock.lang.Shared
import spock.lang.Specification

class ObjectIdSpec extends Specification {

    @Shared BSONMessageCodec codec = new BSONMessageCodec()

    def "ObjectId create from Hex"() {
        setup:
        def a = new ObjectId("4d88e15b60f486e428412dc9")
        def b = new ObjectId("00000000aabbccddee000001")

        expect:
        a.timeStamp == 1300816219
        a.machine   == [0x60, 0xf4, 0x86]*.byteValue() as byte[]
        a.pid       == 0xe428
        a.increment == 4271561

        b.timeStamp == 0
        b.machine   == [0xaa, 0xbb, 0xcc]*.byteValue() as byte[]
        b.pid       == 0xddee
        b.increment == 1
    }

    def "ObjectId create from bin"() {
        setup:
        def idA = [ // timestamp
                   0x4d, 0x88, 0xe1, 0x5b,
                   // machine
                   0x60, 0xf4, 0x86,
                   // pid
                   0xe4, 0x28,
                   // counter
                   0x41, 0x2d, 0xc9]*.byteValue() as byte[]
        def idB = [// timestamp
                   0x00, 0x00, 0x00, 0x00,
                   // machine
                   0xaa, 0xbb, 0xcc,
                   // pid
                   0xdd, 0xee,
                   // counter
                   0x00, 0x00, 0x01]*.byteValue() as byte[]

        def a = new ObjectId(idA)
        def b = new ObjectId(idB)

        expect:
        a.timeStamp == 1300816219
        a.machine   == [0x60, 0xf4, 0x86]*.byteValue() as byte[]
        a.pid       == 0xe428
        a.increment == 4271561

        b.timeStamp == 0
        b.machine   == [0xaa, 0xbb, 0xcc]*.byteValue() as byte[]
        b.pid       == 0xddee
        b.increment == 1
    }

    def "ObjectID to String"() {
        setup:
        def a = new ObjectId("4d88e15b60f486e428412dc9")
        def b = new ObjectId("00000000aabbccddee000001")

        expect:
        a.toString() == "4d88e15b60f486e428412dc9"
        b.toString() == "00000000aabbccddee000001"
    }

    def "ObjectId equality"() {
        setup:
        def oid = new ObjectId("4d88e15b60f486e428412dc9")
        def oid2 = new ObjectId("4d88e15b60f486e428412dc9")

        expect:
        oid == oid2
    }

    def "Encode ObjectId"() {
        setup:
        def doc = new BSONDocument()
        doc["_"] = new ObjectId("4d88e15b60f486e428412dc9")
        def buffer = Buffer.buffer()
        when: 'Encode'
        codec.encodeToWire buffer, doc
        def expected = buffer.bytes
        then: 'Bytes are correct'
        expected == [
                // length
                 0x14, 0x00, 0x00, 0x00,
                 // data
                 0x07, (char)'_', 0x00, 0x4d,
                0x88, 0xe1, 0x5b, 0x60, 0xf4, 0x86, 0xe4, 0x28, 0x41, 0x2d, 0xc9,
                 // end
                 0x00] as byte[]

        when: 'Decode'
        def dec = codec.decodeFromWire(0, Buffer.buffer(expected))
        then:
        dec == doc
    }


}
