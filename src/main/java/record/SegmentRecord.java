package record;

import io.pravega.common.util.BitConverter;
import lombok.Data;

/**
 * get record.SegmentRecord from ZK
 */
@Data
public class SegmentRecord {
    private final int segmentNumber;
    private final long startTime;
    private final double routingKeyStart;
    private final double routingKeyEnd;

    public static SegmentRecord parse(final byte[] table, int offset) {
        return new SegmentRecord(BitConverter.readInt(table, offset),
               BitConverter.readLong(table, Integer.BYTES + offset),
                toDouble(table, Integer.BYTES + Long.BYTES + offset),
                toDouble(table, Integer.BYTES + Long.BYTES + Double.BYTES + offset));
    }

    private static double toDouble(byte[] b, int offset) {
        return Double.longBitsToDouble(BitConverter.readLong(b, offset));
    }




}
