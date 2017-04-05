package za.co.koperfontein.vertxbsoncodec

import groovy.transform.CompileStatic

import java.lang.management.ManagementFactory
import java.security.MessageDigest

@CompileStatic
class ObjectId {
    static final byte[] MACHINE
    static final int PID
    static int counter = 0
    public final byte[] oid

    static {
        String hostname = null
        try {
            hostname = InetAddress.localHost.hostName
        } catch (Exception e) {
        }
        MACHINE = MessageDigest.getInstance("MD5").digest((hostname ?: "localhost").bytes)[0..2] as byte[]

        int pid
        try {
            pid = ManagementFactory.runtimeMXBean.name.split("@")[0].toInteger()
        } catch (Exception e) {
            e.printStackTrace()
            pid = (int)(Math.random() * 0x00ffffff)
        }
        PID = pid
    }

    public ObjectId() {
        this((int) System.currentTimeMillis() / 1000, counter++)
    }

    private ObjectId(int timestamp, int increment) {
        oid =   (
                    (0..3).collect   {(byte)((timestamp >>  ((3-it) * 8)) & 0xff)}
                    + MACHINE[0..2]
                    + (0..1).collect {(byte)((PID >>        ((1-it) * 8)) & 0xff)}
                    + (0..2).collect {(byte)((increment >>  ((2-it) * 8)) & 0xff)}
                ) as byte[]
    }

    ObjectId(String hex) {
        def b = new BigInteger(hex, 16).toByteArray() as List<Byte>
        b = b.size() > 12 ?
                b[0..11]
                : b.size() < 12 ?
                ( (0..<(12-b.size())).collect {0.byteValue()} + b)
                : b
        oid = b as byte[]
    }

    ObjectId(byte[] hex) {
        oid = new byte[12]
        System.arraycopy hex, 0, oid, 0, 12
    }

    int getTimeStamp() {
        (0..3).collect {
            (oid[it] & 0xff) << ((3-it)*8)
        }
        .sum() as int
    }

    Date getDate() {new Date(timeStamp * 1000l)}

    byte[] getMachine() {
        oid[4..6] as byte[]
    }

    int getPid() {
        (0..1).collect {
            (oid[it + 7] & 0xff) << ((1-it)*8)
        }.sum() as int
    }

    int getIncrement() {
        (oid[9] & 0xff) << 16 | (oid[10] & 0xff) << 8 | (oid[11] & 0xff)
    }

    byte[] getBytes() { oid }

    @Override
    int hashCode() {
        (0..<12).inject(0, {result, v -> 31i*result + oid[v]}) as int
    }

    @Override
    boolean equals(Object obj) {
        !(obj == null || obj.class != this.class || (0..<12).any {oid[it] != ((ObjectId)obj).oid[it]})
    }

    @Override
    String toString() {
        oid.encodeHex().toString()
    }
}
