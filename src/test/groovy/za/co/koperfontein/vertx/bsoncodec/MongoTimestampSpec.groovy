package za.co.koperfontein.vertx.bsoncodec

import io.vertx.core.buffer.Buffer
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Timestamp

class MongoTimestampSpec extends Specification {
    @Shared BSONMessageCodec codec = new BSONMessageCodec()

    def "Encode/decode timestamp"() {
        setup:
        def doc = new BSONDocument()
        doc['_'] = new Timestamp(258)
        def buffer = Buffer.buffer()
        when: 'Encode the timestamp'
        codec.encodeToWire(buffer, doc)
        and: 'Get the bytes of the wire'
        def expected = buffer.bytes

        then: 'The bytes should be correct'
        expected == [// length
                     0x10, 0x00, 0x00, 0x00,
                     0x11, (char)'_', 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                     // end
                     0x00] as byte[]

        when: 'Decode'
        def dec = codec.decodeFromWire(0, Buffer.buffer(expected))
        then:
        dec == doc
    }
}
